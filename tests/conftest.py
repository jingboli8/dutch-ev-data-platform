from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

import dutch_ev_platform.pipeline as pipeline_module
from dutch_ev_platform.config import Settings


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        vehicle_url="https://example.test/vehicles",
        fuel_url="https://example.test/fuels",
        snapshot_limit=2,
        page_size=2,
        detail_batch_size=2,
        request_timeout_seconds=1,
        max_retries=1,
        data_dir=tmp_path / "data",
        database_path=tmp_path / "data" / "warehouse.duckdb",
        state_dir=tmp_path / ".state",
        log_level="INFO",
        hash_salt="test-only-salt",
    )


@pytest.fixture(autouse=True)
def stub_dbt_build(monkeypatch):
    """Keep focused pipeline tests fast; dbt itself has dedicated integration tests."""

    def build_test_models(settings: Settings) -> float:
        with duckdb.connect(str(settings.database_path)) as connection:
            connection.execute("CREATE SCHEMA IF NOT EXISTS analytics")
            connection.execute(
                """
                CREATE TABLE analytics.dim_vehicle AS
                SELECT vehicle_id_hash AS vehicle_key
                FROM staging.vehicles
                """
            )
            connection.execute(
                """
                CREATE TABLE analytics.dim_vehicle_model AS
                SELECT vehicle_id_hash AS test_row_key
                FROM staging.vehicles
                """
            )
            connection.execute(
                """
                CREATE TABLE analytics.dim_registration_date AS
                SELECT vehicle_id_hash AS test_row_key
                FROM staging.vehicles
                """
            )
            connection.execute(
                """
                CREATE TABLE analytics.dim_powertrain AS
                SELECT vehicle_id_hash AS test_row_key
                FROM staging.vehicles
                """
            )
            connection.execute(
                """
                CREATE TABLE analytics.fact_vehicle_snapshot AS
                SELECT vehicle_id_hash AS test_row_key
                FROM staging.vehicles
                """
            )
            connection.execute(
                """
                CREATE TABLE analytics.fact_vehicle_fuel AS
                SELECT vehicle_id_hash AS test_row_key
                FROM staging.fuels
                """
            )
            connection.execute(
                """
                CREATE TABLE analytics.mart_ev_overview AS
                SELECT vehicle_id_hash AS test_row_key
                FROM staging.vehicles
                """
            )
            connection.execute(
                """
                CREATE TABLE analytics.mart_ev_metrics AS
                SELECT vehicle_id_hash AS test_row_key
                FROM staging.vehicles
                """
            )
        return 0.01

    monkeypatch.setattr(pipeline_module, "run_dbt_build", build_test_models)


@pytest.fixture
def vehicle_rows() -> list[dict[str, str]]:
    return [
        {
            "kenteken": "TEST_VEHICLE_001",
            "merk": "Tesla",
            "handelsbenaming": "Model 3",
            "datum_eerste_toelating": "20200115",
            "voertuigsoort": "Personenauto",
        },
        {
            "kenteken": "TEST_VEHICLE_002",
            "merk": "Volvo",
            "handelsbenaming": "XC40",
            "datum_eerste_toelating": "20220601",
            "voertuigsoort": "Personenauto",
        },
    ]


@pytest.fixture
def fuel_rows() -> list[dict[str, str]]:
    return [
        {
            "kenteken": "TEST_VEHICLE_001",
            "brandstof_volgnummer": "1",
            "brandstof_omschrijving": "Elektriciteit",
            "nettomaximumvermogen": "153.00",
        },
        {
            "kenteken": "TEST_VEHICLE_002",
            "brandstof_volgnummer": "1",
            "brandstof_omschrijving": "Elektriciteit",
            "nettomaximumvermogen": "110",
        },
        {
            "kenteken": "TEST_VEHICLE_002",
            "brandstof_volgnummer": "2",
            "brandstof_omschrijving": "Benzine",
            "co2_uitstoot_gecombineerd": "48",
        },
    ]
