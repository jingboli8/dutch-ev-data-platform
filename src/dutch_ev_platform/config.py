"""Configuration loading with TOML defaults and environment overrides."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import os
import tomllib


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def _project_path(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


@dataclass(frozen=True)
class Settings:
    vehicle_url: str
    fuel_url: str
    snapshot_limit: int | None
    page_size: int
    detail_batch_size: int
    request_timeout_seconds: int
    max_retries: int
    data_dir: Path
    database_path: Path
    state_dir: Path
    log_level: str
    hash_salt: str | None

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def parquet_dir(self) -> Path:
        return self.data_dir / "parquet"

    @property
    def checkpoint_path(self) -> Path:
        return self.state_dir / "checkpoints" / "rdw_ev_snapshot.json"

    @classmethod
    def load(cls, config_path: Path | None = None) -> "Settings":
        _load_dotenv(PROJECT_ROOT / ".env")
        path = config_path or PROJECT_ROOT / "config" / "settings.toml"
        with path.open("rb") as handle:
            config = tomllib.load(handle)
        api = config["api"]
        paths = config["paths"]
        logging_config = config["logging"]
        configured_limit = int(
            os.getenv("EV_SNAPSHOT_LIMIT", api["snapshot_limit"])
        )
        return cls(
            vehicle_url=os.getenv("EV_VEHICLE_URL", api["vehicle_url"]),
            fuel_url=os.getenv("EV_FUEL_URL", api["fuel_url"]),
            snapshot_limit=configured_limit if configured_limit > 0 else None,
            page_size=int(os.getenv("EV_API_PAGE_SIZE", api["page_size"])),
            detail_batch_size=int(
                os.getenv("EV_DETAIL_BATCH_SIZE", api["detail_batch_size"])
            ),
            request_timeout_seconds=int(
                os.getenv("EV_REQUEST_TIMEOUT_SECONDS", api["request_timeout_seconds"])
            ),
            max_retries=int(os.getenv("EV_MAX_RETRIES", api["max_retries"])),
            data_dir=_project_path(os.getenv("EV_DATA_DIR", paths["data_dir"])),
            database_path=_project_path(
                os.getenv("EV_DATABASE_PATH", paths["database_path"])
            ),
            state_dir=_project_path(
                os.getenv("EV_STATE_DIR", paths["state_dir"])
            ),
            log_level=os.getenv("EV_LOG_LEVEL", logging_config["level"]).upper(),
            hash_salt=os.getenv("EV_HASH_SALT"),
        )
