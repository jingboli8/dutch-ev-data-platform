"""End-to-end orchestration for the local RDW pipeline."""

from __future__ import annotations

import logging
from pathlib import Path
import uuid

import duckdb

from .config import Settings
from .extract import RDWClient
from .storage import (
    get_hash_salt,
    initialize_metadata,
    persist_raw_payload,
    utc_now,
)
from .transform import (
    build_models,
    export_parquet,
    normalize_fuel_rows,
    normalize_vehicle_rows,
    run_quality_checks,
)


LOGGER = logging.getLogger(__name__)


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


def run_pipeline(settings: Settings, client: RDWClient | None = None) -> dict[str, object]:
    ingestion_id = f"{utc_now():%Y%m%dT%H%M%SZ}_{uuid.uuid4().hex[:8]}"
    settings.database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(settings.database_path))
    initialize_metadata(connection)
    connection.execute(
        "INSERT INTO meta.ingestion_runs VALUES (?, ?, NULL, 'running', ?, 0, 0, 0, NULL)",
        [ingestion_id, utc_now(), settings.sample_limit],
    )
    LOGGER.info(
        "Pipeline started",
        extra={"event": "pipeline_start", "ingestion_id": ingestion_id},
    )
    try:
        rdw = client or RDWClient(settings)
        identifiers = rdw.fetch_ev_identifier_sample(settings.sample_limit)
        if not identifiers:
            raise RuntimeError("The RDW fuel sample returned no EV identifiers")
        vehicle_raw = rdw.fetch_vehicles_by_ids(identifiers)
        if not vehicle_raw:
            raise RuntimeError("No vehicle rows matched the sampled EV identifiers")
        fuel_raw = rdw.fetch_fuels_for_vehicles(identifiers)
        duplicate_payloads = 0
        for dataset, rows in (("vehicles", vehicle_raw), ("fuels", fuel_raw)):
            raw_path, digest, _ = persist_raw_payload(
                settings.raw_dir, dataset, ingestion_id, rows
            )
            duplicate_payloads += int(
                _record_payload(
                    connection, dataset, digest, raw_path, len(rows), ingestion_id
                )
            )
            LOGGER.info(
                "Raw payload persisted",
                extra={
                    "event": "raw_persisted",
                    "ingestion_id": ingestion_id,
                    "dataset": dataset,
                    "row_count": len(rows),
                    "path": str(raw_path),
                },
            )
        salt = get_hash_salt(settings)
        vehicles = normalize_vehicle_rows(vehicle_raw, salt, ingestion_id)
        fuels = normalize_fuel_rows(fuel_raw, salt, ingestion_id)
        build_models(connection, vehicles, fuels)
        checks = run_quality_checks(connection)
        export_parquet(connection, settings.parquet_dir)
        ev_count = connection.execute(
            "SELECT count(*) FROM analytics.ev_vehicles"
        ).fetchone()[0]
        connection.execute(
            """
            UPDATE meta.ingestion_runs
            SET completed_at = ?, status = 'succeeded', vehicle_rows = ?,
                fuel_rows = ?, duplicate_payloads = ?
            WHERE ingestion_id = ?
            """,
            [
                utc_now(),
                len(vehicle_raw),
                len(fuel_raw),
                duplicate_payloads,
                ingestion_id,
            ],
        )
        LOGGER.info(
            "Pipeline completed",
            extra={
                "event": "pipeline_success",
                "ingestion_id": ingestion_id,
                "row_count": ev_count,
            },
        )
        return {
            "ingestion_id": ingestion_id,
            "vehicle_rows": len(vehicle_raw),
            "fuel_rows": len(fuel_raw),
            "ev_rows": ev_count,
            "duplicate_payloads": duplicate_payloads,
            "quality_checks": checks,
            "database_path": str(settings.database_path),
        }
    except Exception as exc:
        connection.execute(
            """
            UPDATE meta.ingestion_runs
            SET completed_at = ?, status = 'failed', error_message = ?
            WHERE ingestion_id = ?
            """,
            [utc_now(), str(exc)[:2000], ingestion_id],
        )
        LOGGER.exception(
            "Pipeline failed",
            extra={"event": "pipeline_failure", "ingestion_id": ingestion_id},
        )
        raise
    finally:
        connection.close()
