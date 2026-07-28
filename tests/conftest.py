from __future__ import annotations

from pathlib import Path

import pytest

from dutch_ev_platform.config import Settings


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        vehicle_url="https://example.test/vehicles",
        fuel_url="https://example.test/fuels",
        sample_limit=2,
        page_size=100,
        request_timeout_seconds=1,
        max_retries=1,
        data_dir=tmp_path / "data",
        database_path=tmp_path / "data" / "warehouse.duckdb",
        log_level="INFO",
        hash_salt="test-only-salt",
    )


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
