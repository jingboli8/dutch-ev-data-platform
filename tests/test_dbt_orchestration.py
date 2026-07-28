from __future__ import annotations

import hashlib
import os
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

import dutch_ev_platform.dbt_orchestration as dbt_module
from dutch_ev_platform.dbt_orchestration import (
    REQUIRED_DBT_RELATIONS,
    DbtBuildError,
    export_dbt_parquet,
    run_dbt_command,
)


def test_dbt_executable_must_be_beside_active_python(
    tmp_path, monkeypatch
):
    python_dir = tmp_path / "environment with spaces" / "bin"
    python_dir.mkdir(parents=True)
    python_path = python_dir / ("python.exe" if os.name == "nt" else "python")
    monkeypatch.setattr(dbt_module.sys, "executable", str(python_path))

    with pytest.raises(DbtBuildError, match="active project environment"):
        dbt_module._dbt_executable()

    dbt_path = python_dir / ("dbt.exe" if os.name == "nt" else "dbt")
    dbt_path.write_text("", encoding="utf-8")
    assert dbt_module._dbt_executable() == dbt_path


def test_dbt_subprocess_uses_safe_local_arguments_and_environment(
    settings, tmp_path, monkeypatch
):
    local_dbt = tmp_path / "environment with spaces" / "dbt"
    local_dbt.parent.mkdir()
    local_dbt.write_text("", encoding="utf-8")
    monkeypatch.setattr(dbt_module, "_dbt_executable", lambda: local_dbt)
    monkeypatch.setenv("INHERITED_TEST_MARKER", "preserved")
    captured = {}

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured.update(kwargs)
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(dbt_module.subprocess, "run", fake_run)
    run_dbt_command(settings, ["build"])

    assert captured["command"][0] == str(local_dbt)
    assert captured["command"][1] == "build"
    assert captured["command"][-5:] == [
        "--project-dir",
        str(dbt_module.DBT_PROJECT_DIR),
        "--profiles-dir",
        str(dbt_module.DBT_PROJECT_DIR),
        "--no-use-colors",
    ]
    assert captured["cwd"] == dbt_module.PROJECT_ROOT
    assert captured["env"]["INHERITED_TEST_MARKER"] == "preserved"
    assert captured["env"]["DBT_DUCKDB_PATH"] == str(
        settings.database_path.resolve()
    )
    assert captured.get("shell", False) is False


def test_parquet_publication_preserves_previous_set_on_export_failure(
    settings, monkeypatch
):
    settings.database_path.parent.mkdir(parents=True, exist_ok=True)
    settings.parquet_dir.mkdir(parents=True)
    (settings.parquet_dir / "obsolete.parquet").write_bytes(b"obsolete")
    with duckdb.connect(str(settings.database_path)) as connection:
        connection.execute("CREATE SCHEMA analytics")
        for relation in REQUIRED_DBT_RELATIONS:
            connection.execute(
                f'CREATE TABLE analytics."{relation}" AS '
                "SELECT ?::VARCHAR AS marker",
                [relation],
            )
        export_dbt_parquet(connection, settings.parquet_dir)
        assert not (
            settings.parquet_dir / "obsolete.parquet"
        ).exists()

        before = {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in settings.parquet_dir.glob("*.parquet")
        }
        original_write = dbt_module._write_parquet_relation
        writes = 0

        def fail_second_write(connection, relation, target):
            nonlocal writes
            writes += 1
            if writes == 2:
                raise OSError("simulated staged export failure")
            return original_write(connection, relation, target)

        monkeypatch.setattr(
            dbt_module, "_write_parquet_relation", fail_second_write
        )
        with pytest.raises(OSError, match="staged export"):
            export_dbt_parquet(connection, settings.parquet_dir)

    after = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in settings.parquet_dir.glob("*.parquet")
    }
    assert before == after
    assert len(after) == 8
    assert not list(
        settings.parquet_dir.parent.glob(
            f".{settings.parquet_dir.name}.publish-*"
        )
    )


def test_parquet_publication_restores_previous_set_if_swap_fails(
    settings, monkeypatch
):
    settings.database_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(settings.database_path)) as connection:
        connection.execute("CREATE SCHEMA analytics")
        for relation in REQUIRED_DBT_RELATIONS:
            connection.execute(
                f'CREATE TABLE analytics."{relation}" AS '
                "SELECT ?::VARCHAR AS marker",
                [relation],
            )
        export_dbt_parquet(connection, settings.parquet_dir)
        before = {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in settings.parquet_dir.glob("*.parquet")
        }
        original_replace = Path.replace

        def fail_publication_swap(path, target):
            if (
                path.name.startswith(
                    f".{settings.parquet_dir.name}.publish-"
                )
                and Path(target) == settings.parquet_dir
            ):
                raise OSError("simulated directory swap failure")
            return original_replace(path, target)

        monkeypatch.setattr(Path, "replace", fail_publication_swap)
        with pytest.raises(OSError, match="directory swap"):
            export_dbt_parquet(connection, settings.parquet_dir)

    after = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in settings.parquet_dir.glob("*.parquet")
    }
    assert before == after
    assert len(after) == 8
