"""Raw response persistence, hashing, and ingestion metadata."""

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import secrets
from typing import Any

import duckdb

from .config import Settings


def canonical_json_bytes(rows: list[dict[str, Any]]) -> bytes:
    return json.dumps(
        rows, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def payload_hash(rows: list[dict[str, Any]]) -> str:
    return hashlib.sha256(canonical_json_bytes(rows)).hexdigest()


def persist_raw_payload(
    raw_dir: Path,
    dataset: str,
    ingestion_id: str,
    rows: list[dict[str, Any]],
) -> tuple[Path, str, bool]:
    del ingestion_id  # Payloads are content-addressed, not duplicated per run.
    content = canonical_json_bytes(rows)
    digest = hashlib.sha256(content).hexdigest()
    target_dir = raw_dir / dataset
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / f"{digest}.json"
    created = not target.exists()
    if created:
        temporary = target.with_suffix(".tmp")
        temporary.write_bytes(content)
        temporary.replace(target)
    elif hashlib.sha256(target.read_bytes()).hexdigest() != digest:
        raise RuntimeError(
            "An existing raw payload failed its SHA-256 integrity check. "
            "Remove the private raw cache and use --fresh to recover."
        )
    return target, digest, created


def load_raw_payload(
    raw_dir: Path, dataset: str, digest: str
) -> list[dict[str, Any]]:
    if len(digest) != 64 or any(
        character not in "0123456789abcdef" for character in digest
    ):
        raise RuntimeError(
            "The checkpoint raw-page digest is invalid. Use --fresh to recover."
        )
    path = raw_dir / dataset / f"{digest}.json"
    try:
        content = path.read_bytes()
        if hashlib.sha256(content).hexdigest() != digest:
            raise RuntimeError(
                "The checkpoint raw page failed its SHA-256 integrity check. "
                "Use --fresh to recover."
            )
        value = json.loads(content.decode("utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError(
            "The raw anchor page required for resume is missing or invalid. "
            "Use --fresh to start a new snapshot."
        ) from exc
    if not isinstance(value, list) or not all(isinstance(row, dict) for row in value):
        raise RuntimeError("The raw anchor page is not a JSON array of objects")
    return value


def get_hash_salt(settings: Settings) -> str:
    if settings.hash_salt:
        return settings.hash_salt
    settings.state_dir.mkdir(parents=True, exist_ok=True)
    path = settings.state_dir / "privacy_salt"
    if not path.exists():
        path.write_text(secrets.token_hex(32), encoding="utf-8")
    return path.read_text(encoding="utf-8").strip()


def hash_vehicle_id(vehicle_id: str, salt: str) -> str:
    return hashlib.sha256(f"{salt}:{vehicle_id}".encode("utf-8")).hexdigest()


def initialize_metadata(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("CREATE SCHEMA IF NOT EXISTS meta")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS meta.ingestion_runs (
            ingestion_id VARCHAR PRIMARY KEY,
            started_at TIMESTAMPTZ NOT NULL,
            completed_at TIMESTAMPTZ,
            status VARCHAR NOT NULL,
            sample_limit INTEGER NOT NULL,
            vehicle_rows INTEGER DEFAULT 0,
            fuel_rows INTEGER DEFAULT 0,
            duplicate_payloads INTEGER DEFAULT 0,
            error_message VARCHAR,
            ingestion_mode VARCHAR,
            requested_limit BIGINT,
            page_size INTEGER,
            pages_requested INTEGER DEFAULT 0,
            source_rows_received BIGINT DEFAULT 0,
            matched_vehicles BIGINT DEFAULT 0,
            rejected_rows BIGINT DEFAULT 0,
            active_duration_seconds DOUBLE DEFAULT 0,
            wall_clock_elapsed_seconds DOUBLE DEFAULT 0,
            processed_rows_per_second DOUBLE DEFAULT 0,
            checkpoint_status VARCHAR,
            resumed BOOLEAN DEFAULT false,
            resume_count INTEGER DEFAULT 0
        )
        """
    )
    migrations = {
        "ingestion_mode": "VARCHAR",
        "requested_limit": "BIGINT",
        "page_size": "INTEGER",
        "pages_requested": "INTEGER DEFAULT 0",
        "source_rows_received": "BIGINT DEFAULT 0",
        "matched_vehicles": "BIGINT DEFAULT 0",
        "rejected_rows": "BIGINT DEFAULT 0",
        "active_duration_seconds": "DOUBLE DEFAULT 0",
        "wall_clock_elapsed_seconds": "DOUBLE DEFAULT 0",
        "processed_rows_per_second": "DOUBLE DEFAULT 0",
        "checkpoint_status": "VARCHAR",
        "resumed": "BOOLEAN DEFAULT false",
        "resume_count": "INTEGER DEFAULT 0",
    }
    for column, definition in migrations.items():
        connection.execute(
            f"ALTER TABLE meta.ingestion_runs "
            f"ADD COLUMN IF NOT EXISTS {column} {definition}"
        )
    columns = {
        row[1]
        for row in connection.execute(
            "PRAGMA table_info('meta.ingestion_runs')"
        ).fetchall()
    }
    if (
        "run_duration_seconds" in columns
        and "active_duration_seconds" in columns
    ):
        connection.execute(
            """
            UPDATE meta.ingestion_runs
            SET active_duration_seconds = run_duration_seconds
            WHERE active_duration_seconds = 0
            """
        )
        connection.execute(
            "ALTER TABLE meta.ingestion_runs DROP COLUMN run_duration_seconds"
        )
    if "rows_per_second" in columns and "processed_rows_per_second" in columns:
        connection.execute(
            """
            UPDATE meta.ingestion_runs
            SET processed_rows_per_second = rows_per_second
            WHERE processed_rows_per_second = 0
            """
        )
        connection.execute(
            "ALTER TABLE meta.ingestion_runs DROP COLUMN rows_per_second"
        )
    connection.execute(
        """
        UPDATE meta.ingestion_runs
        SET ingestion_mode = 'legacy_sample'
        WHERE checkpoint_status IS NULL
          AND (ingestion_mode IS NULL OR ingestion_mode = 'resumable_snapshot')
        """
    )
    connection.execute(
        """
        UPDATE meta.ingestion_runs
        SET requested_limit = NULLIF(sample_limit, 0)
        WHERE requested_limit IS NULL
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS meta.ingested_payloads (
            dataset VARCHAR NOT NULL,
            payload_sha256 VARCHAR NOT NULL,
            raw_path VARCHAR NOT NULL,
            ingested_at TIMESTAMPTZ NOT NULL,
            row_count INTEGER NOT NULL,
            first_ingestion_id VARCHAR NOT NULL,
            PRIMARY KEY (dataset, payload_sha256)
        )
        """
    )


def utc_now() -> datetime:
    return datetime.now(timezone.utc)
