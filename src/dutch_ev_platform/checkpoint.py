"""Privacy-safe checkpoint state for resumable snapshot ingestion."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

CHECKPOINT_VERSION = 1


class CheckpointError(RuntimeError):
    """Raised when checkpoint state is missing, incompatible, or unsafe."""


@dataclass
class SnapshotCheckpoint:
    version: int
    mode: str
    status: str
    ingestion_id: str
    started_at: str
    requested_limit: int | None
    page_size: int
    completed_pages: int = 0
    source_rows_received: int = 0
    matched_vehicles: int = 0
    fuel_rows: int = 0
    rejected_rows: int = 0
    duplicate_payloads: int = 0
    pages_requested: int = 0
    active_duration_seconds: float = 0.0
    last_anchor_payload_sha256: str | None = None
    configuration_sha256: str | None = None
    resumed: bool = False
    resume_count: int = 0

    @classmethod
    def new(
        cls,
        ingestion_id: str,
        started_at: datetime,
        requested_limit: int | None,
        page_size: int,
        configuration_sha256: str,
    ) -> "SnapshotCheckpoint":
        return cls(
            version=CHECKPOINT_VERSION,
            mode="resumable_snapshot",
            status="in_progress",
            ingestion_id=ingestion_id,
            started_at=started_at.isoformat(),
            requested_limit=requested_limit,
            page_size=page_size,
            configuration_sha256=configuration_sha256,
        )

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SnapshotCheckpoint":
        checkpoint = cls(**value)
        if checkpoint.version != CHECKPOINT_VERSION:
            raise CheckpointError(
                f"Unsupported checkpoint version: {checkpoint.version}"
            )
        if checkpoint.mode != "resumable_snapshot":
            raise CheckpointError(f"Unsupported checkpoint mode: {checkpoint.mode}")
        return checkpoint


class CheckpointStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def exists(self) -> bool:
        return self.path.exists()

    def load(self) -> SnapshotCheckpoint:
        if not self.path.exists():
            raise CheckpointError(
                f"No checkpoint exists at {self.path}. Start with --fresh."
            )
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise CheckpointError(f"Checkpoint cannot be read: {exc}") from exc
        if not isinstance(value, dict):
            raise CheckpointError("Checkpoint must contain a JSON object")
        return SnapshotCheckpoint.from_dict(value)

    def save(self, checkpoint: SnapshotCheckpoint) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(
            json.dumps(asdict(checkpoint), indent=2, sort_keys=True),
            encoding="utf-8",
        )
        temporary.replace(self.path)


def assert_checkpoint_compatible(
    checkpoint: SnapshotCheckpoint,
    requested_limit: int | None,
    page_size: int,
    configuration_sha256: str,
) -> None:
    if checkpoint.requested_limit != requested_limit:
        raise CheckpointError(
            "The resume limit differs from the checkpoint. "
            "Reuse the original --limit value."
        )
    if checkpoint.page_size != page_size:
        raise CheckpointError(
            "The resume page size differs from the checkpoint. "
            "Reuse the original --page-size value."
        )
    if checkpoint.configuration_sha256 is None:
        raise CheckpointError(
            "The checkpoint predates configuration integrity checks. "
            "Use --fresh to start a safe snapshot."
        )
    if checkpoint.configuration_sha256 != configuration_sha256:
        raise CheckpointError(
            "Resume configuration differs from the checkpoint. Endpoint, data "
            "location, database, detail batch, and hash salt must remain unchanged."
        )
