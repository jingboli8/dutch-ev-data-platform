from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import duckdb
import pytest

import dutch_ev_platform.pipeline as pipeline_module
from dutch_ev_platform.checkpoint import CheckpointError, CheckpointStore
from dutch_ev_platform.dbt_orchestration import DbtBuildError
from dutch_ev_platform.extract import ExtractionError
from dutch_ev_platform.pipeline import run_pipeline, run_transform_only

IDENTIFIERS = [
    "TEST_VEHICLE_001",
    "TEST_VEHICLE_002",
    "TEST_VEHICLE_003",
]


def _vehicle(identifier: str) -> dict[str, str]:
    return {
        "kenteken": identifier,
        "merk": "Test Brand",
        "handelsbenaming": f"Model {identifier[-1]}",
        "datum_eerste_toelating": "20220115",
        "voertuigsoort": "Personenauto",
    }


def _fuel(identifier: str) -> dict[str, str]:
    return {
        "kenteken": identifier,
        "brandstof_volgnummer": "1",
        "brandstof_omschrijving": "Elektriciteit",
        "nettomaximumvermogen": "100",
    }


class FakePagedClient:
    def __init__(self, identifiers=None):
        self.identifiers = IDENTIFIERS if identifiers is None else identifiers
        self.request_count = 0

    def fetch_ev_identifier_page(self, limit, after_identifier=None):
        self.request_count += 1
        start = (
            self.identifiers.index(after_identifier) + 1
            if after_identifier in self.identifiers
            else 0
        )
        return [
            {"kenteken": value}
            for value in self.identifiers[start : start + limit]
        ]

    def fetch_vehicle_pages(self, vehicle_ids):
        self.request_count += 1
        yield [_vehicle(value) for value in vehicle_ids]

    def fetch_fuel_pages(self, vehicle_ids):
        self.request_count += 1
        yield [_fuel(value) for value in vehicle_ids]


class InterruptAfterFirstPage(FakePagedClient):
    def fetch_ev_identifier_page(self, limit, after_identifier=None):
        if after_identifier is not None:
            self.request_count += 1
            raise ExtractionError("simulated interruption")
        return super().fetch_ev_identifier_page(limit, after_identifier)


class DuplicatePageClient(FakePagedClient):
    def fetch_ev_identifier_page(self, limit, after_identifier=None):
        self.request_count += 1
        return [{"kenteken": value} for value in self.identifiers[:limit]]


class DuplicateIdentifierClient(FakePagedClient):
    def fetch_ev_identifier_page(self, limit, after_identifier=None):
        self.request_count += 1
        return [{"kenteken": self.identifiers[0]}] * min(limit, 2)


class FailAfterAnchorClient(FakePagedClient):
    def fetch_vehicle_pages(self, vehicle_ids):
        self.request_count += 1
        raise ExtractionError("simulated failure after raw anchor persistence")
        yield


class NoRequestClient(FakePagedClient):
    def fetch_ev_identifier_page(self, limit, after_identifier=None):
        raise AssertionError("a completed checkpoint must not request another page")


def _fail_dbt_build(settings):
    with duckdb.connect(str(settings.database_path), read_only=True) as connection:
        assert connection.execute(
            "SELECT count(*) FROM staging.vehicles"
        ).fetchone()[0] > 0
    raise DbtBuildError("simulated dbt transformation failure")


def _row_counts(database_path: Path) -> tuple[int, int, int]:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        fact_exists = connection.execute(
            """
            SELECT count(*)
            FROM information_schema.tables
            WHERE table_schema = 'analytics'
              AND table_name = 'fact_vehicle_snapshot'
            """
        ).fetchone()[0]
        return (
            connection.execute(
                "SELECT count(*) FROM staging.vehicles"
            ).fetchone()[0],
            connection.execute(
                "SELECT count(*) FROM staging.fuels"
            ).fetchone()[0],
            (
                connection.execute(
                    "SELECT count(*) FROM analytics.fact_vehicle_snapshot"
                ).fetchone()[0]
                if fact_exists
                else 0
            ),
        )


def _known_analytics_count(database_path: Path) -> int:
    with duckdb.connect(str(database_path), read_only=True) as connection:
        return connection.execute(
            """
            SELECT count(*)
            FROM information_schema.tables
            WHERE table_schema IN (
                'dbt_staging', 'intermediate', 'analytics'
            )
              AND table_name IN (
                'stg_vehicles', 'stg_fuels', 'stg_ingestion_runs',
                'int_vehicle_fuel_profile', 'int_snapshot_context',
                'dim_vehicle', 'dim_vehicle_model',
                'dim_registration_date', 'dim_powertrain',
                'fact_vehicle_snapshot', 'fact_vehicle_fuel',
                'mart_ev_overview', 'mart_ev_metrics'
              )
            """
        ).fetchone()[0]


def test_multiple_pages_build_private_outputs(settings):
    configured = replace(settings, snapshot_limit=3, page_size=2)

    result = run_pipeline(configured, FakePagedClient(), fresh=True)

    assert result["completed_pages"] == 2
    assert result["matched_vehicles"] == 3
    assert result["fuel_rows"] == 3
    assert result["ev_rows"] == 3
    assert result["processed_rows"] == 6
    assert result["active_duration_seconds"] > 0
    assert (
        result["wall_clock_elapsed_seconds"]
        >= result["active_duration_seconds"]
    )
    assert result["processed_rows_per_second"] == pytest.approx(
        result["processed_rows"] / result["active_duration_seconds"],
        rel=0.02,
    )
    assert result["checkpoint_status"] == "completed"
    assert result["quality_checks"]["plain_identifier_columns"] == 0
    assert (
        configured.parquet_dir / "analytics_mart_ev_metrics.parquet"
    ).exists()
    with duckdb.connect(str(configured.database_path), read_only=True) as connection:
        assert connection.execute(
            "SELECT count(*) FROM staging.vehicles"
        ).fetchone()[0] == 3


def test_interruption_and_resume_uses_private_checkpoint(settings):
    configured = replace(settings, snapshot_limit=3, page_size=2)

    with pytest.raises(ExtractionError, match="simulated interruption"):
        run_pipeline(configured, InterruptAfterFirstPage(), fresh=True)

    interrupted = configured.checkpoint_path.read_text(encoding="utf-8")
    assert "TEST_VEHICLE" not in interrupted
    assert "kenteken" not in interrupted.lower()
    assert json.loads(interrupted)["status"] == "interrupted"

    result = run_pipeline(configured, FakePagedClient(), resume=True)

    assert result["resumed"] is True
    assert result["resume_count"] == 1
    assert result["matched_vehicles"] == 3
    assert result["checkpoint_status"] == "completed"
    completed = configured.checkpoint_path.read_text(encoding="utf-8")
    assert "TEST_VEHICLE" not in completed
    assert "kenteken" not in completed.lower()


def test_duplicate_page_is_rejected_without_checkpoint_identifier_leak(settings):
    configured = replace(settings, snapshot_limit=3, page_size=2)

    with pytest.raises(ExtractionError, match="did not advance"):
        run_pipeline(configured, DuplicatePageClient(), fresh=True)

    checkpoint = configured.checkpoint_path.read_text(encoding="utf-8")
    assert "TEST_VEHICLE" not in checkpoint


def test_duplicate_identifiers_within_page_are_rejected(settings):
    with pytest.raises(ExtractionError, match="strictly ordered and unique"):
        run_pipeline(settings, DuplicateIdentifierClient(), fresh=True)
    assert _row_counts(settings.database_path) == (0, 0, 0)


def test_empty_terminal_page_completes_unlimited_snapshot(settings):
    configured = replace(settings, snapshot_limit=None, page_size=2)
    client = FakePagedClient(identifiers=IDENTIFIERS[:2])

    result = run_pipeline(configured, client, fresh=True)

    assert result["completed_pages"] == 1
    assert result["matched_vehicles"] == 2
    assert result["pages_requested"] == 4


def test_fresh_idempotent_rerun_audits_duplicate_payloads(settings):
    client = FakePagedClient(identifiers=IDENTIFIERS[:2])
    first = run_pipeline(settings, client, fresh=True)
    second = run_pipeline(
        settings, FakePagedClient(identifiers=IDENTIFIERS[:2]), fresh=True
    )

    assert first["duplicate_payloads"] == 0
    assert second["duplicate_payloads"] == 3
    with duckdb.connect(str(settings.database_path), read_only=True) as connection:
        assert connection.execute(
            "SELECT count(*) FROM staging.vehicles"
        ).fetchone()[0] == 2
        assert connection.execute(
            "SELECT count(*) FROM meta.ingestion_runs WHERE status = 'succeeded'"
        ).fetchone()[0] == 2


def test_fresh_smaller_snapshot_removes_rows_from_previous_snapshot(settings):
    larger = replace(settings, snapshot_limit=3, page_size=2)
    smaller = replace(settings, snapshot_limit=1, page_size=1)

    run_pipeline(larger, FakePagedClient(), fresh=True)
    result = run_pipeline(smaller, FakePagedClient(), fresh=True)

    assert result["matched_vehicles"] == 1
    assert result["ev_rows"] == 1
    assert _row_counts(settings.database_path) == (1, 1, 1)
    with duckdb.connect() as connection:
        parquet_count = connection.execute(
            "SELECT count(*) FROM read_parquet(?)",
            [
                str(
                    settings.parquet_dir
                    / "analytics_fact_vehicle_snapshot.parquet"
                )
            ],
        ).fetchone()[0]
    assert parquet_count == 1


def test_interrupted_fresh_initialization_is_safely_resumable(
    settings, monkeypatch
):
    run_pipeline(
        replace(settings, snapshot_limit=3, page_size=2),
        FakePagedClient(),
        fresh=True,
    )
    original_initialize = pipeline_module._initialize_fresh_snapshot

    def crash_during_initialization(connection, checkpoint, configured_settings):
        del connection, checkpoint, configured_settings
        raise KeyboardInterrupt("simulated fresh initialization crash")

    monkeypatch.setattr(
        pipeline_module, "_initialize_fresh_snapshot", crash_during_initialization
    )
    smaller = replace(settings, snapshot_limit=1, page_size=1)
    with pytest.raises(KeyboardInterrupt, match="initialization"):
        run_pipeline(smaller, FakePagedClient(), fresh=True)
    monkeypatch.setattr(
        pipeline_module, "_initialize_fresh_snapshot", original_initialize
    )

    assert json.loads(
        settings.checkpoint_path.read_text(encoding="utf-8")
    )["status"] == "initializing"
    result = run_pipeline(smaller, FakePagedClient(), resume=True)
    assert result["matched_vehicles"] == 1
    assert _row_counts(settings.database_path) == (1, 1, 1)


def test_resume_replays_raw_page_after_failure_before_staging(settings):
    with pytest.raises(ExtractionError, match="after raw anchor"):
        run_pipeline(settings, FailAfterAnchorClient(), fresh=True)

    assert _row_counts(settings.database_path) == (0, 0, 0)
    result = run_pipeline(settings, FakePagedClient(), resume=True)

    assert result["matched_vehicles"] == 2
    assert result["duplicate_payloads"] >= 1
    assert _row_counts(settings.database_path) == (2, 2, 2)


def test_resume_replays_committed_page_if_checkpoint_save_crashes(
    settings, monkeypatch
):
    original_save = CheckpointStore.save
    save_calls = 0

    def crash_after_page_commit(self, checkpoint):
        nonlocal save_calls
        save_calls += 1
        if save_calls == 3:
            raise KeyboardInterrupt("simulated checkpoint write crash")
        return original_save(self, checkpoint)

    monkeypatch.setattr(CheckpointStore, "save", crash_after_page_commit)
    with pytest.raises(KeyboardInterrupt, match="checkpoint write"):
        run_pipeline(settings, FakePagedClient(), fresh=True)
    monkeypatch.setattr(CheckpointStore, "save", original_save)

    assert _row_counts(settings.database_path)[:2] == (2, 2)
    result = run_pipeline(settings, FakePagedClient(), resume=True)

    assert result["matched_vehicles"] == 2
    assert _row_counts(settings.database_path) == (2, 2, 2)


def test_resume_after_checkpoint_advance_before_metadata_update(
    settings, monkeypatch
):
    original_update = pipeline_module._update_run
    update_calls = 0

    def crash_before_metadata(*args, **kwargs):
        nonlocal update_calls
        update_calls += 1
        if update_calls == 1:
            raise KeyboardInterrupt("simulated metadata update crash")
        return original_update(*args, **kwargs)

    monkeypatch.setattr(pipeline_module, "_update_run", crash_before_metadata)
    with pytest.raises(KeyboardInterrupt, match="metadata update"):
        run_pipeline(settings, FakePagedClient(), fresh=True)
    monkeypatch.setattr(pipeline_module, "_update_run", original_update)

    result = run_pipeline(settings, FakePagedClient(), resume=True)
    assert result["matched_vehicles"] == 2
    assert _row_counts(settings.database_path) == (2, 2, 2)


def test_completed_checkpoint_reconciles_metadata_without_api_request(
    settings, monkeypatch
):
    original_update = pipeline_module._update_run

    def crash_before_success(connection, checkpoint, status, error_message=None):
        if status == "succeeded":
            raise KeyboardInterrupt("simulated final metadata crash")
        return original_update(connection, checkpoint, status, error_message)

    monkeypatch.setattr(
        pipeline_module, "_update_run", crash_before_success
    )
    with pytest.raises(KeyboardInterrupt, match="final metadata"):
        run_pipeline(settings, FakePagedClient(), fresh=True)
    monkeypatch.setattr(pipeline_module, "_update_run", original_update)

    assert json.loads(
        settings.checkpoint_path.read_text(encoding="utf-8")
    )["status"] == "completed"
    result = run_pipeline(settings, NoRequestClient(), resume=True)
    assert result["checkpoint_status"] == "completed"
    assert result["matched_vehicles"] == 2
    with duckdb.connect(
        str(settings.database_path), read_only=True
    ) as connection:
        assert connection.execute(
            "SELECT status FROM meta.ingestion_runs"
        ).fetchone()[0] == "succeeded"


def test_failed_dbt_build_resumes_without_requesting_rdw_pages(settings):
    with pytest.raises(DbtBuildError, match="simulated dbt"):
        run_pipeline(
            settings,
            FakePagedClient(identifiers=IDENTIFIERS[:2]),
            fresh=True,
            dbt_executor=_fail_dbt_build,
        )

    checkpoint = json.loads(
        settings.checkpoint_path.read_text(encoding="utf-8")
    )
    assert checkpoint["status"] == "transformation_failed"
    assert _row_counts(settings.database_path)[:2] == (2, 2)
    with duckdb.connect(
        str(settings.database_path), read_only=True
    ) as connection:
        assert connection.execute(
            "SELECT status FROM meta.ingestion_runs"
        ).fetchone()[0] == "transformation_failed"

    result = run_pipeline(settings, NoRequestClient(), resume=True)

    assert result["checkpoint_status"] == "completed"
    assert result["matched_vehicles"] == 2
    assert result["resume_count"] == 1
    assert _row_counts(settings.database_path) == (2, 2, 2)


def test_partial_dbt_build_is_removed_and_resumes_without_api(settings):
    def fail_after_partial_model(configured):
        with duckdb.connect(str(configured.database_path)) as connection:
            connection.execute("CREATE SCHEMA IF NOT EXISTS analytics")
            connection.execute(
                "CREATE TABLE analytics.dim_vehicle(vehicle_key VARCHAR)"
            )
        raise DbtBuildError("simulated partial dbt failure")

    with pytest.raises(DbtBuildError, match="partial dbt"):
        run_pipeline(
            settings,
            FakePagedClient(identifiers=IDENTIFIERS[:2]),
            fresh=True,
            dbt_executor=fail_after_partial_model,
        )

    assert _known_analytics_count(settings.database_path) == 0
    assert _row_counts(settings.database_path)[:2] == (2, 2)
    result = run_pipeline(settings, NoRequestClient(), resume=True)
    assert result["checkpoint_status"] == "completed"
    assert _row_counts(settings.database_path) == (2, 2, 2)


def test_parquet_export_failure_is_recoverable_without_api(
    settings, monkeypatch
):
    original_export = pipeline_module.export_dbt_parquet

    def fail_export(connection, parquet_dir):
        del connection
        parquet_dir.mkdir(parents=True, exist_ok=True)
        (parquet_dir / "partial.parquet.tmp").write_bytes(b"partial")
        raise OSError("simulated Parquet publication failure")

    monkeypatch.setattr(
        pipeline_module, "export_dbt_parquet", fail_export
    )
    with pytest.raises(OSError, match="Parquet publication"):
        run_pipeline(
            settings,
            FakePagedClient(identifiers=IDENTIFIERS[:2]),
            fresh=True,
        )

    checkpoint = json.loads(
        settings.checkpoint_path.read_text(encoding="utf-8")
    )
    assert checkpoint["status"] == "transformation_failed"
    assert _known_analytics_count(settings.database_path) == 0
    assert not list(settings.parquet_dir.glob("*.parquet"))

    monkeypatch.setattr(
        pipeline_module, "export_dbt_parquet", original_export
    )
    result = run_pipeline(settings, NoRequestClient(), resume=True)
    assert result["checkpoint_status"] == "completed"
    assert _row_counts(settings.database_path) == (2, 2, 2)


def test_final_metadata_failure_is_recoverable_without_api(
    settings, monkeypatch
):
    original_update = pipeline_module._update_run
    failed = False

    def fail_once_after_dbt(
        connection, checkpoint, status, error_message=None
    ):
        nonlocal failed
        if status == "finalizing" and not failed:
            failed = True
            raise RuntimeError("simulated post-dbt metadata failure")
        return original_update(
            connection, checkpoint, status, error_message
        )

    monkeypatch.setattr(pipeline_module, "_update_run", fail_once_after_dbt)
    with pytest.raises(RuntimeError, match="post-dbt metadata"):
        run_pipeline(
            settings,
            FakePagedClient(identifiers=IDENTIFIERS[:2]),
            fresh=True,
        )

    checkpoint = json.loads(
        settings.checkpoint_path.read_text(encoding="utf-8")
    )
    assert checkpoint["status"] == "transformation_failed"
    with duckdb.connect(
        str(settings.database_path), read_only=True
    ) as connection:
        assert connection.execute(
            "SELECT status FROM meta.ingestion_runs"
        ).fetchone()[0] == "transformation_failed"
    assert _known_analytics_count(settings.database_path) == 0

    monkeypatch.setattr(
        pipeline_module, "_update_run", original_update
    )
    result = run_pipeline(settings, NoRequestClient(), resume=True)
    assert result["checkpoint_status"] == "completed"
    assert _row_counts(settings.database_path) == (2, 2, 2)


def test_interrupt_between_close_and_dbt_start_resumes_without_api(settings):
    def interrupt_before_dbt(configured):
        del configured
        raise KeyboardInterrupt("simulated pre-dbt interrupt")

    with pytest.raises(KeyboardInterrupt, match="pre-dbt"):
        run_pipeline(
            settings,
            FakePagedClient(identifiers=IDENTIFIERS[:2]),
            fresh=True,
            dbt_executor=interrupt_before_dbt,
        )

    checkpoint = json.loads(
        settings.checkpoint_path.read_text(encoding="utf-8")
    )
    assert checkpoint["status"] == "staging_complete"
    assert _row_counts(settings.database_path)[:2] == (2, 2)
    result = run_pipeline(settings, NoRequestClient(), resume=True)
    assert result["checkpoint_status"] == "completed"
    assert _row_counts(settings.database_path) == (2, 2, 2)


def test_interrupt_after_dbt_before_completion_resumes_without_api(
    settings, monkeypatch
):
    completed_build = pipeline_module.run_dbt_build
    original_inspect = pipeline_module.inspect_dbt_outputs

    def interrupt_after_build(connection):
        assert connection.execute(
            """
            SELECT count(*) FROM information_schema.tables
            WHERE table_schema = 'analytics'
              AND table_name = 'fact_vehicle_snapshot'
            """
        ).fetchone()[0] == 1
        raise KeyboardInterrupt("simulated post-dbt interrupt")

    monkeypatch.setattr(
        pipeline_module, "inspect_dbt_outputs", interrupt_after_build
    )
    with pytest.raises(KeyboardInterrupt, match="post-dbt"):
        run_pipeline(
            settings,
            FakePagedClient(identifiers=IDENTIFIERS[:2]),
            fresh=True,
            dbt_executor=completed_build,
        )

    checkpoint = json.loads(
        settings.checkpoint_path.read_text(encoding="utf-8")
    )
    assert checkpoint["status"] == "staging_complete"
    assert _row_counts(settings.database_path)[:2] == (2, 2)

    monkeypatch.setattr(
        pipeline_module, "inspect_dbt_outputs", original_inspect
    )
    result = run_pipeline(settings, NoRequestClient(), resume=True)
    assert result["checkpoint_status"] == "completed"
    assert _row_counts(settings.database_path) == (2, 2, 2)


def test_phase1_analytical_tables_are_removed_during_migration(settings):
    run_pipeline(
        settings,
        FakePagedClient(identifiers=IDENTIFIERS[:2]),
        fresh=True,
    )
    with duckdb.connect(str(settings.database_path)) as connection:
        connection.execute(
            """
            CREATE TABLE analytics.ev_vehicles AS
            SELECT vehicle_id_hash FROM staging.vehicles
            """
        )
        connection.execute(
            """
            CREATE TABLE analytics.ev_fuel_details AS
            SELECT vehicle_id_hash FROM staging.fuels
            """
        )
        connection.execute(
            "CREATE TABLE analytics.ev_metrics(metric_value INTEGER)"
        )
    settings.checkpoint_path.unlink()

    result = run_transform_only(settings)

    assert result["mode"] == "transform_only"
    assert result["dbt_model_rows"]["fact_vehicle_snapshot"] == 2
    with duckdb.connect(
        str(settings.database_path), read_only=True
    ) as connection:
        legacy = connection.execute(
            """
            SELECT count(*)
            FROM information_schema.tables
            WHERE table_schema = 'analytics'
              AND table_name IN (
                  'ev_vehicles', 'ev_fuel_details', 'ev_metrics'
              )
            """
        ).fetchone()[0]
    assert legacy == 0


def test_transform_only_preserves_staging_and_unrelated_checkpoint(settings):
    run_pipeline(
        settings,
        FakePagedClient(identifiers=IDENTIFIERS[:2]),
        fresh=True,
    )
    checkpoint_before = settings.checkpoint_path.read_bytes()
    with duckdb.connect(
        str(settings.database_path), read_only=True
    ) as connection:
        vehicles_before = connection.execute(
            "SELECT * FROM staging.vehicles ORDER BY vehicle_id_hash"
        ).fetchall()
        fuels_before = connection.execute(
            """
            SELECT * FROM staging.fuels
            ORDER BY vehicle_id_hash, fuel_sequence
            """
        ).fetchall()

    first = run_transform_only(settings)
    second = run_transform_only(settings)

    assert first["dbt_model_rows"] == second["dbt_model_rows"]
    assert settings.checkpoint_path.read_bytes() == checkpoint_before
    with duckdb.connect(
        str(settings.database_path), read_only=True
    ) as connection:
        assert connection.execute(
            "SELECT * FROM staging.vehicles ORDER BY vehicle_id_hash"
        ).fetchall() == vehicles_before
        assert connection.execute(
            """
            SELECT * FROM staging.fuels
            ORDER BY vehicle_id_hash, fuel_sequence
            """
        ).fetchall() == fuels_before


def test_transform_only_export_failure_cleans_models_and_is_repeatable(
    settings, monkeypatch
):
    run_pipeline(
        settings,
        FakePagedClient(identifiers=IDENTIFIERS[:2]),
        fresh=True,
    )
    checkpoint_before = settings.checkpoint_path.read_bytes()
    counts_before = _row_counts(settings.database_path)[:2]
    original_export = pipeline_module.export_dbt_parquet

    def fail_export(connection, parquet_dir):
        del connection, parquet_dir
        raise OSError("simulated transform-only export failure")

    monkeypatch.setattr(
        pipeline_module, "export_dbt_parquet", fail_export
    )
    with pytest.raises(OSError, match="transform-only export"):
        run_transform_only(settings)

    assert _known_analytics_count(settings.database_path) == 0
    assert _row_counts(settings.database_path)[:2] == counts_before
    assert settings.checkpoint_path.read_bytes() == checkpoint_before

    monkeypatch.setattr(
        pipeline_module, "export_dbt_parquet", original_export
    )
    result = run_transform_only(settings)
    assert result["dbt_model_rows"]["fact_vehicle_snapshot"] == 2
    assert _row_counts(settings.database_path)[:2] == counts_before


def test_transform_only_rejects_wrong_database_without_creating_staging(
    settings,
):
    settings.database_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(settings.database_path)) as connection:
        connection.execute("CREATE TABLE unrelated(value INTEGER)")

    with pytest.raises(
        pipeline_module.DataQualityError, match="missing staging.vehicles"
    ):
        run_transform_only(settings)

    with duckdb.connect(
        str(settings.database_path), read_only=True
    ) as connection:
        schemas = {
            row[0]
            for row in connection.execute(
                """
                SELECT schema_name FROM information_schema.schemata
                """
            ).fetchall()
        }
        assert "staging" not in schemas
        assert "meta" not in schemas
        assert connection.execute(
            "SELECT count(*) FROM unrelated"
        ).fetchone()[0] == 0


def test_transform_only_rejects_incompatible_staging_without_mutation(
    settings,
):
    settings.database_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(settings.database_path)) as connection:
        connection.execute("CREATE SCHEMA staging")
        connection.execute(
            "CREATE TABLE staging.vehicles(vehicle_id_hash INTEGER)"
        )
        connection.execute(
            "CREATE TABLE staging.fuels(vehicle_id_hash VARCHAR)"
        )

    with pytest.raises(
        pipeline_module.DataQualityError, match="staging schema is incompatible"
    ):
        run_transform_only(settings)

    with duckdb.connect(
        str(settings.database_path), read_only=True
    ) as connection:
        assert connection.execute(
            """
            SELECT data_type
            FROM information_schema.columns
            WHERE table_schema = 'staging'
              AND table_name = 'vehicles'
              AND column_name = 'vehicle_id_hash'
            """
        ).fetchone()[0] == "INTEGER"
        assert connection.execute(
            """
            SELECT count(*)
            FROM information_schema.schemata
            WHERE schema_name = 'meta'
            """
        ).fetchone()[0] == 0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("vehicle_url", "https://changed.test/vehicles"),
        ("fuel_url", "https://changed.test/fuels"),
        ("detail_batch_size", 1),
        ("data_dir", Path("different-data")),
        ("database_path", Path("different.duckdb")),
        ("hash_salt", "different-test-salt"),
    ],
)
def test_resume_rejects_snapshot_identity_configuration_changes(
    settings, field, value
):
    configured = replace(settings, snapshot_limit=3, page_size=2)
    with pytest.raises(ExtractionError):
        run_pipeline(configured, InterruptAfterFirstPage(), fresh=True)

    changed = replace(configured, **{field: value})
    with pytest.raises(CheckpointError, match="configuration differs"):
        run_pipeline(changed, FakePagedClient(), resume=True)

    assert _row_counts(configured.database_path)[:2] == (2, 2)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("snapshot_limit", 4, "resume limit differs"),
        ("page_size", 1, "resume page size differs"),
    ],
)
def test_resume_rejects_limit_and_page_size_changes(
    settings, field, value, message
):
    configured = replace(settings, snapshot_limit=3, page_size=2)
    with pytest.raises(ExtractionError):
        run_pipeline(configured, InterruptAfterFirstPage(), fresh=True)

    with pytest.raises(CheckpointError, match=message):
        run_pipeline(
            replace(configured, **{field: value}),
            FakePagedClient(),
            resume=True,
        )


@pytest.mark.parametrize("damage", ["missing", "truncated", "modified"])
def test_resume_rejects_missing_or_corrupt_checkpoint_anchor(
    settings, damage, caplog
):
    configured = replace(settings, snapshot_limit=3, page_size=2)
    with pytest.raises(ExtractionError):
        run_pipeline(configured, InterruptAfterFirstPage(), fresh=True)
    checkpoint = json.loads(
        configured.checkpoint_path.read_text(encoding="utf-8")
    )
    raw_path = (
        configured.raw_dir
        / "ev_identifiers"
        / f"{checkpoint['last_anchor_payload_sha256']}.json"
    )
    if damage == "missing":
        raw_path.unlink()
    elif damage == "truncated":
        raw_path.write_text("[", encoding="utf-8")
    else:
        raw_path.write_text('[{"kenteken":"PRIVATE_PLATE"}]', encoding="utf-8")

    with pytest.raises(RuntimeError, match="raw anchor|integrity"):
        run_pipeline(configured, FakePagedClient(), resume=True)

    logs = caplog.text
    assert "PRIVATE_PLATE" not in logs
    assert all(identifier not in logs for identifier in IDENTIFIERS)


def test_empty_first_page_fails_without_stale_analytics(settings):
    with pytest.raises(
        pipeline_module.DataQualityError, match="no matching vehicles"
    ):
        run_pipeline(
            settings, FakePagedClient(identifiers=[]), fresh=True
        )
    assert _row_counts(settings.database_path) == (0, 0, 0)


def test_checkpoint_and_parquet_never_contain_plain_identifiers(settings):
    run_pipeline(settings, FakePagedClient(identifiers=IDENTIFIERS[:2]), fresh=True)

    checkpoint = settings.checkpoint_path.read_text(encoding="utf-8")
    assert all(identifier not in checkpoint for identifier in IDENTIFIERS)
    with duckdb.connect() as connection:
        for parquet in settings.parquet_dir.glob("*.parquet"):
            columns = {
                row[0]
                for row in connection.execute(
                    "DESCRIBE SELECT * FROM read_parquet(?)", [str(parquet)]
                ).fetchall()
            }
            assert "kenteken" not in {column.lower() for column in columns}
