"""Run repository-local dbt safely and publish its analytical outputs."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
import uuid
from collections.abc import Sequence
from pathlib import Path

import duckdb

from .config import PROJECT_ROOT, Settings

DBT_PROJECT_DIR = PROJECT_ROOT / "dbt"
REQUIRED_DBT_RELATIONS = {
    "dim_vehicle",
    "dim_vehicle_model",
    "dim_registration_date",
    "dim_powertrain",
    "fact_vehicle_snapshot",
    "fact_vehicle_fuel",
    "mart_ev_overview",
    "mart_ev_metrics",
}


class DbtBuildError(RuntimeError):
    """Raised when dbt cannot create a valid analytical layer."""


def _dbt_executable() -> Path:
    name = "dbt.exe" if os.name == "nt" else "dbt"
    beside_python = Path(sys.executable).with_name(name)
    if beside_python.exists():
        return beside_python
    raise DbtBuildError(
        "dbt is not installed in the active project environment"
    )


def run_dbt_command(
    settings: Settings,
    arguments: Sequence[str],
) -> float:
    """Invoke dbt without a shell and return measured process duration."""
    environment = os.environ.copy()
    environment["DBT_DUCKDB_PATH"] = str(settings.database_path.resolve())
    command = [
        str(_dbt_executable()),
        *arguments,
        "--project-dir",
        str(DBT_PROJECT_DIR),
        "--profiles-dir",
        str(DBT_PROJECT_DIR),
        "--no-use-colors",
    ]
    started = time.perf_counter()
    completed = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        env=environment,
        check=False,
    )
    duration = time.perf_counter() - started
    if completed.returncode != 0:
        raise DbtBuildError(
            f"dbt {' '.join(arguments)} failed with exit code "
            f"{completed.returncode}; inspect ignored dbt logs for details"
        )
    return duration


def run_dbt_build(settings: Settings) -> float:
    return run_dbt_command(settings, ["build"])


def inspect_dbt_outputs(
    connection: duckdb.DuckDBPyConnection,
) -> dict[str, int]:
    rows = connection.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'analytics'
          AND table_type = 'BASE TABLE'
        """
    ).fetchall()
    available = {row[0] for row in rows}
    missing = REQUIRED_DBT_RELATIONS - available
    if missing:
        raise DbtBuildError(
            "dbt completed without required analytical relations: "
            + ", ".join(sorted(missing))
        )
    return {
        relation: connection.execute(
            f'SELECT count(*) FROM analytics."{relation}"'
        ).fetchone()[0]
        for relation in sorted(REQUIRED_DBT_RELATIONS)
    }


def clear_generated_parquet(parquet_dir: Path) -> None:
    parent = parquet_dir.parent
    for abandoned in parent.glob(f".{parquet_dir.name}.publish-*"):
        if abandoned.is_dir():
            shutil.rmtree(abandoned)
    for backup in parent.glob(f".{parquet_dir.name}.backup-*"):
        if backup.is_dir():
            shutil.rmtree(backup)
    if not parquet_dir.exists():
        return
    for path in parquet_dir.glob("*.parquet"):
        path.unlink()
    for path in parquet_dir.glob("*.parquet.tmp"):
        path.unlink()


def _write_parquet_relation(
    connection: duckdb.DuckDBPyConnection,
    relation: str,
    target: Path,
) -> None:
    connection.execute(
        f'COPY analytics."{relation}" TO ? '
        "(FORMAT PARQUET, COMPRESSION ZSTD)",
        [target.as_posix()],
    )


def export_dbt_parquet(
    connection: duckdb.DuckDBPyConnection,
    parquet_dir: Path,
) -> list[str]:
    """Publish one complete set of dbt-owned tables through a directory swap."""
    rows = connection.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'analytics'
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
    ).fetchall()
    relations = [row[0] for row in rows if row[0] in REQUIRED_DBT_RELATIONS]
    if set(relations) != REQUIRED_DBT_RELATIONS:
        raise DbtBuildError("Cannot export an incomplete dbt analytical layer")

    parquet_dir.parent.mkdir(parents=True, exist_ok=True)
    publication_id = uuid.uuid4().hex
    temporary_dir = (
        parquet_dir.parent / f".{parquet_dir.name}.publish-{publication_id}"
    )
    backup_dir = (
        parquet_dir.parent / f".{parquet_dir.name}.backup-{publication_id}"
    )
    temporary_dir.mkdir()
    filenames = [
        f"analytics_{relation}.parquet" for relation in relations
    ]
    try:
        for relation, filename in zip(relations, filenames, strict=True):
            _write_parquet_relation(
                connection, relation, temporary_dir / filename
            )
        if parquet_dir.exists():
            parquet_dir.replace(backup_dir)
        try:
            temporary_dir.replace(parquet_dir)
        except BaseException:
            if backup_dir.exists() and not parquet_dir.exists():
                backup_dir.replace(parquet_dir)
            raise
        if backup_dir.exists():
            shutil.rmtree(backup_dir)
        for abandoned in parquet_dir.parent.glob(
            f".{parquet_dir.name}.publish-*"
        ):
            if abandoned.is_dir():
                shutil.rmtree(abandoned)
        for abandoned in parquet_dir.parent.glob(
            f".{parquet_dir.name}.backup-*"
        ):
            if abandoned.is_dir():
                shutil.rmtree(abandoned)
    except BaseException:
        if temporary_dir.exists():
            shutil.rmtree(temporary_dir)
        raise
    return sorted(filenames)
