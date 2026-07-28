"""End-to-end orchestration for resumable RDW EV snapshot ingestion."""

from __future__ import annotations

from datetime import datetime
import hashlib
import json
import logging
from pathlib import Path
import time
import uuid

import duckdb

from .checkpoint import (
    CheckpointStore,
    SnapshotCheckpoint,
    assert_checkpoint_compatible,
)
from .config import Settings
from .extract import ExtractionError, RDWClient
from .storage import (
    get_hash_salt,
    initialize_metadata,
    load_raw_payload,
    persist_raw_payload,
    utc_now,
)
from .transform import (
    DataQualityError,
    build_analytics,
    clear_staging,
    export_parquet,
    initialize_model_tables,
    normalize_fuel_rows,
    normalize_vehicle_rows,
    run_quality_checks,
    upsert_staging_page,
)


LOGGER = logging.getLogger(__name__)
ANCHOR_DATASET = "ev_identifiers"
SNAPSHOT_QUERY_VERSION = 1


def _configuration_sha256(settings: Settings, salt: str) -> str:
    """Bind a checkpoint to every setting that affects snapshot identity."""
    configuration = {
        "snapshot_query_version": SNAPSHOT_QUERY_VERSION,
        "vehicle_url": settings.vehicle_url,
        "fuel_url": settings.fuel_url,
        "data_dir": str(settings.data_dir.resolve()),
        "database_path": str(settings.database_path.resolve()),
        "requested_limit": settings.snapshot_limit,
        "page_size": settings.page_size,
        "detail_batch_size": settings.detail_batch_size,
        "hash_salt_sha256": hashlib.sha256(salt.encode("utf-8")).hexdigest(),
    }
    encoded = json.dumps(
        configuration, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _wall_clock_elapsed_seconds(checkpoint: SnapshotCheckpoint) -> float:
    started_at = datetime.fromisoformat(checkpoint.started_at)
    return max(0.0, (utc_now() - started_at).total_seconds())


def _record_payload(
    connection: duckdb.DuckDBPyConnection,
    dataset: str,
    digest: str,
    path: Path,
    row_count: int,
    ingestion_id: str,
) -> bool:
    exists = connection.execute(
        """
        SELECT 1 FROM meta.ingested_payloads
        WHERE dataset = ? AND payload_sha256 = ?
        """,
        [dataset, digest],
    ).fetchone()
    if exists:
        return True
    connection.execute(
        """
        INSERT INTO meta.ingested_payloads
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [dataset, digest, str(path), utc_now(), row_count, ingestion_id],
    )
    return False


def _persist_and_audit(
    connection: duckdb.DuckDBPyConnection,
    settings: Settings,
    dataset: str,
    ingestion_id: str,
    rows: list[dict[str, object]],
) -> tuple[str, bool]:
    path, digest, _ = persist_raw_payload(
        settings.raw_dir, dataset, ingestion_id, rows
    )
    duplicate = _record_payload(
        connection, dataset, digest, path, len(rows), ingestion_id
    )
    LOGGER.info(
        "Raw API page persisted",
        extra={
            "event": "raw_page_persisted",
            "ingestion_id": ingestion_id,
            "dataset": dataset,
            "row_count": len(rows),
        },
    )
    return digest, duplicate


def _identifier_rows(rows: list[dict[str, object]]) -> list[str]:
    return [
        str(row.get("kenteken", "")).strip()
        for row in rows
        if str(row.get("kenteken", "")).strip()
    ]


def _resume_cursor(settings: Settings, checkpoint: SnapshotCheckpoint) -> str | None:
    digest = checkpoint.last_anchor_payload_sha256
    if digest is None:
        return None
    rows = load_raw_payload(settings.raw_dir, ANCHOR_DATASET, digest)
    identifiers = _identifier_rows(rows)
    if not identifiers:
        raise RuntimeError("The checkpoint anchor page contains no usable identifier")
    return identifiers[-1]


def _create_run(
    connection: duckdb.DuckDBPyConnection,
    checkpoint: SnapshotCheckpoint,
) -> None:
    connection.execute(
        """
        INSERT INTO meta.ingestion_runs (
            ingestion_id, started_at, completed_at, status, sample_limit,
            vehicle_rows, fuel_rows, duplicate_payloads, error_message,
            ingestion_mode, requested_limit, page_size, pages_requested,
            source_rows_received,
            matched_vehicles, rejected_rows, active_duration_seconds,
            wall_clock_elapsed_seconds, processed_rows_per_second,
            checkpoint_status, resumed, resume_count
        )
        VALUES (?, ?, NULL, 'running', ?, 0, 0, 0, NULL,
                'resumable_snapshot', ?, ?, 0, 0, 0, 0, 0, 0, 0,
                'in_progress', false, 0)
        """,
        [
            checkpoint.ingestion_id,
            checkpoint.started_at,
            checkpoint.requested_limit or 0,
            checkpoint.requested_limit,
            checkpoint.page_size,
        ],
    )


def _initialize_fresh_snapshot(
    connection: duckdb.DuckDBPyConnection,
    checkpoint: SnapshotCheckpoint,
) -> None:
    """Atomically replace the current warehouse snapshot and run record."""
    connection.execute("BEGIN TRANSACTION")
    try:
        clear_staging(connection)
        build_analytics(connection)
        connection.execute(
            "DELETE FROM meta.ingestion_runs WHERE ingestion_id = ?",
            [checkpoint.ingestion_id],
        )
        _create_run(connection, checkpoint)
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise


def _update_run(
    connection: duckdb.DuckDBPyConnection,
    checkpoint: SnapshotCheckpoint,
    status: str,
    error_message: str | None = None,
) -> None:
    duration = checkpoint.active_duration_seconds
    wall_clock_elapsed = _wall_clock_elapsed_seconds(checkpoint)
    processed = checkpoint.matched_vehicles + checkpoint.fuel_rows
    throughput = processed / duration if duration > 0 else 0.0
    completed_at = utc_now() if status in {"succeeded", "interrupted"} else None
    connection.execute(
        """
        UPDATE meta.ingestion_runs
        SET completed_at = ?, status = ?, vehicle_rows = ?, fuel_rows = ?,
            duplicate_payloads = ?, error_message = ?, pages_requested = ?,
            source_rows_received = ?, matched_vehicles = ?, rejected_rows = ?,
            active_duration_seconds = ?, wall_clock_elapsed_seconds = ?,
            processed_rows_per_second = ?,
            checkpoint_status = ?, resumed = ?, resume_count = ?
        WHERE ingestion_id = ?
        """,
        [
            completed_at,
            status,
            checkpoint.matched_vehicles,
            checkpoint.fuel_rows,
            checkpoint.duplicate_payloads,
            error_message,
            checkpoint.pages_requested,
            checkpoint.source_rows_received,
            checkpoint.matched_vehicles,
            checkpoint.rejected_rows,
            duration,
            wall_clock_elapsed,
            throughput,
            checkpoint.status,
            checkpoint.resumed,
            checkpoint.resume_count,
            checkpoint.ingestion_id,
        ],
    )


def _result(
    checkpoint: SnapshotCheckpoint,
    settings: Settings,
    quality_checks: dict[str, int],
    ev_rows: int,
) -> dict[str, object]:
    duration = checkpoint.active_duration_seconds
    wall_clock_elapsed = _wall_clock_elapsed_seconds(checkpoint)
    processed = checkpoint.matched_vehicles + checkpoint.fuel_rows
    return {
        "ingestion_id": checkpoint.ingestion_id,
        "ingestion_mode": checkpoint.mode,
        "requested_limit": checkpoint.requested_limit,
        "page_size": checkpoint.page_size,
        "completed_pages": checkpoint.completed_pages,
        "pages_requested": checkpoint.pages_requested,
        "source_rows_received": checkpoint.source_rows_received,
        "matched_vehicles": checkpoint.matched_vehicles,
        "fuel_rows": checkpoint.fuel_rows,
        "rejected_rows": checkpoint.rejected_rows,
        "duplicate_payloads": checkpoint.duplicate_payloads,
        "ev_rows": ev_rows,
        "processed_rows": processed,
        "active_duration_seconds": round(duration, 3),
        "wall_clock_elapsed_seconds": round(wall_clock_elapsed, 3),
        "processed_rows_per_second": (
            round(processed / duration, 3) if duration > 0 else 0.0
        ),
        "checkpoint_status": checkpoint.status,
        "resumed": checkpoint.resumed,
        "resume_count": checkpoint.resume_count,
        "quality_checks": quality_checks,
        "database_path": str(settings.database_path),
    }


def run_pipeline(
    settings: Settings,
    client: RDWClient | None = None,
    *,
    resume: bool = False,
    fresh: bool = False,
) -> dict[str, object]:
    """Run or resume one bounded-memory RDW EV snapshot."""
    if resume == fresh:
        raise ValueError("Choose exactly one of resume=True or fresh=True")
    if not 1 <= settings.page_size <= 50_000:
        raise ValueError("page_size must be between 1 and 50000")
    if settings.detail_batch_size < 1:
        raise ValueError("detail_batch_size must be positive")

    salt = get_hash_salt(settings)
    configuration_sha256 = _configuration_sha256(settings, salt)
    checkpoint_store = CheckpointStore(settings.checkpoint_path)
    completed_checkpoint = False

    if resume:
        checkpoint = checkpoint_store.load()
        assert_checkpoint_compatible(
            checkpoint,
            settings.snapshot_limit,
            settings.page_size,
            configuration_sha256,
        )
        completed_checkpoint = checkpoint.status == "completed"

    settings.database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(settings.database_path))
    initialize_metadata(connection)
    initialize_model_tables(connection)
    invocation_started = time.perf_counter()

    try:
        if resume:
            if checkpoint.status == "initializing":
                _initialize_fresh_snapshot(connection, checkpoint)
                checkpoint.status = "in_progress"
                checkpoint_store.save(checkpoint)
            elif not completed_checkpoint:
                checkpoint.status = "in_progress"
            checkpoint.resumed = True
            checkpoint.resume_count += 1
            connection.execute(
                """
                UPDATE meta.ingestion_runs
                SET completed_at = NULL, status = 'running', error_message = NULL,
                    resumed = true, resume_count = ?
                WHERE ingestion_id = ?
                """,
                [checkpoint.resume_count, checkpoint.ingestion_id],
            )
        else:
            started_at = utc_now()
            checkpoint = SnapshotCheckpoint.new(
                ingestion_id=f"{started_at:%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:8]}",
                started_at=started_at,
                requested_limit=settings.snapshot_limit,
                page_size=settings.page_size,
                configuration_sha256=configuration_sha256,
            )
            checkpoint.status = "initializing"
            checkpoint_store.save(checkpoint)
            _initialize_fresh_snapshot(connection, checkpoint)
            checkpoint.status = "in_progress"
            checkpoint_store.save(checkpoint)
    except BaseException:
        connection.close()
        raise

    base_duration = checkpoint.active_duration_seconds
    base_requests = checkpoint.pages_requested
    rdw = client or RDWClient(settings)
    initial_client_requests = getattr(rdw, "request_count", 0)

    try:
        cursor = _resume_cursor(settings, checkpoint)
        LOGGER.info(
            "Snapshot pipeline started",
            extra={
                "event": "pipeline_start",
                "ingestion_id": checkpoint.ingestion_id,
                "checkpoint_status": checkpoint.status,
                "resumed": checkpoint.resumed,
            },
        )
        while not completed_checkpoint:
            if (
                checkpoint.requested_limit is not None
                and checkpoint.matched_vehicles >= checkpoint.requested_limit
            ):
                break
            remaining = (
                checkpoint.requested_limit - checkpoint.matched_vehicles
                if checkpoint.requested_limit is not None
                else checkpoint.page_size
            )
            page_limit = min(checkpoint.page_size, remaining)
            anchor_rows = rdw.fetch_ev_identifier_page(page_limit, cursor)
            anchor_digest, anchor_duplicate = _persist_and_audit(
                connection,
                settings,
                ANCHOR_DATASET,
                checkpoint.ingestion_id,
                anchor_rows,
            )
            if not anchor_rows:
                break

            identifiers = _identifier_rows(anchor_rows)
            if identifiers != sorted(set(identifiers)):
                raise ExtractionError(
                    "RDW anchor page is not strictly ordered and unique"
                )
            if cursor is not None and identifiers[0] <= cursor:
                raise ExtractionError(
                    "RDW keyset page did not advance; stopping to prevent a loop"
                )

            page_source_rows = len(anchor_rows)
            page_duplicate_payloads = int(anchor_duplicate)
            vehicle_raw: list[dict[str, object]] = []
            for rows in rdw.fetch_vehicle_pages(identifiers):
                _, duplicate = _persist_and_audit(
                    connection,
                    settings,
                    "vehicles",
                    checkpoint.ingestion_id,
                    rows,
                )
                page_source_rows += len(rows)
                page_duplicate_payloads += int(duplicate)
                vehicle_raw.extend(rows)

            fuel_raw: list[dict[str, object]] = []
            for rows in rdw.fetch_fuel_pages(identifiers):
                _, duplicate = _persist_and_audit(
                    connection,
                    settings,
                    "fuels",
                    checkpoint.ingestion_id,
                    rows,
                )
                page_source_rows += len(rows)
                page_duplicate_payloads += int(duplicate)
                fuel_raw.extend(rows)

            matched_identifiers = {
                str(row.get("kenteken", "")).strip()
                for row in vehicle_raw
                if str(row.get("kenteken", "")).strip()
            }
            fuel_identifiers = {
                str(row.get("kenteken", "")).strip()
                for row in fuel_raw
                if str(row.get("kenteken", "")).strip()
            }
            complete_identifiers = (
                set(identifiers) & matched_identifiers & fuel_identifiers
            )
            complete_vehicle_raw = [
                row
                for row in vehicle_raw
                if str(row.get("kenteken", "")).strip() in complete_identifiers
            ]
            complete_fuel_raw = [
                row
                for row in fuel_raw
                if str(row.get("kenteken", "")).strip() in complete_identifiers
            ]
            vehicles = normalize_vehicle_rows(
                complete_vehicle_raw, salt, checkpoint.ingestion_id
            )
            fuels = normalize_fuel_rows(
                complete_fuel_raw, salt, checkpoint.ingestion_id
            )
            rejected = (
                len(anchor_rows)
                - len(identifiers)
                + len(set(identifiers) - complete_identifiers)
                + len(complete_vehicle_raw)
                - len(vehicles)
                + len(complete_fuel_raw)
                - len(fuels)
            )
            if not vehicles:
                raise DataQualityError(
                    "An anchor page produced no valid matching vehicle rows"
                )

            connection.execute("BEGIN TRANSACTION")
            try:
                upsert_staging_page(connection, vehicles, fuels)
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise

            checkpoint.completed_pages += 1
            checkpoint.source_rows_received += page_source_rows
            checkpoint.matched_vehicles += len(vehicles)
            checkpoint.fuel_rows += len(fuels)
            checkpoint.rejected_rows += rejected
            checkpoint.duplicate_payloads += page_duplicate_payloads
            checkpoint.last_anchor_payload_sha256 = anchor_digest
            checkpoint.pages_requested = base_requests + (
                getattr(rdw, "request_count", 0) - initial_client_requests
            )
            checkpoint.active_duration_seconds = base_duration + (
                time.perf_counter() - invocation_started
            )
            checkpoint_store.save(checkpoint)
            _update_run(connection, checkpoint, "running")
            cursor = identifiers[-1]

            LOGGER.info(
                "Snapshot page completed",
                extra={
                    "event": "page_completed",
                    "ingestion_id": checkpoint.ingestion_id,
                    "page_number": checkpoint.completed_pages,
                    "row_count": len(vehicles),
                    "pages_requested": checkpoint.pages_requested,
                },
            )
            if len(anchor_rows) < page_limit:
                break

        if checkpoint.matched_vehicles == 0:
            raise DataQualityError("The RDW snapshot returned no matching vehicles")
        build_analytics(connection)
        quality_checks = run_quality_checks(connection)
        export_parquet(connection, settings.parquet_dir)
        ev_rows = connection.execute(
            "SELECT count(*) FROM analytics.ev_vehicles"
        ).fetchone()[0]
        checkpoint.status = "completed"
        checkpoint.pages_requested = base_requests + (
            getattr(rdw, "request_count", 0) - initial_client_requests
        )
        checkpoint.active_duration_seconds = base_duration + (
            time.perf_counter() - invocation_started
        )
        checkpoint_store.save(checkpoint)
        _update_run(connection, checkpoint, "succeeded")
        result = _result(checkpoint, settings, quality_checks, ev_rows)
        LOGGER.info(
            "Snapshot pipeline completed",
            extra={
                "event": "pipeline_success",
                "ingestion_id": checkpoint.ingestion_id,
                "row_count": ev_rows,
                "pages_requested": checkpoint.pages_requested,
                "active_duration_seconds": result["active_duration_seconds"],
                "wall_clock_elapsed_seconds": result[
                    "wall_clock_elapsed_seconds"
                ],
                "processed_rows_per_second": result[
                    "processed_rows_per_second"
                ],
                "checkpoint_status": checkpoint.status,
                "resumed": checkpoint.resumed,
            },
        )
        return result
    except Exception as exc:
        checkpoint.status = "interrupted"
        checkpoint.pages_requested = base_requests + (
            getattr(rdw, "request_count", 0) - initial_client_requests
        )
        checkpoint.active_duration_seconds = base_duration + (
            time.perf_counter() - invocation_started
        )
        checkpoint_store.save(checkpoint)
        _update_run(
            connection, checkpoint, "interrupted", error_message=str(exc)[:2000]
        )
        LOGGER.exception(
            "Snapshot pipeline interrupted",
            extra={
                "event": "pipeline_interrupted",
                "ingestion_id": checkpoint.ingestion_id,
                "pages_requested": checkpoint.pages_requested,
                "checkpoint_status": checkpoint.status,
                "resumed": checkpoint.resumed,
            },
        )
        raise
    finally:
        connection.close()
