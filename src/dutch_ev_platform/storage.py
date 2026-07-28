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
        target.write_bytes(content)
    return target, digest, created


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
            error_message VARCHAR
        )
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
