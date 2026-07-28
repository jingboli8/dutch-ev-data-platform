"""Build an explicitly synthetic, privacy-safe DuckDB fixture for dbt CI."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from dutch_ev_platform.storage import initialize_metadata
from dutch_ev_platform.transform import (
    clear_staging,
    drop_known_analytical_relations,
    initialize_model_tables,
    normalize_fuel_rows,
    normalize_vehicle_rows,
    upsert_staging_page,
)

FIXTURE_INGESTION_ID = "ci_synthetic_snapshot"
FIXTURE_SALT = "CI_SYNTHETIC_SALT_NOT_FOR_REAL_DATA"


def _assert_fixture_path(path: Path) -> None:
    filename = path.name.lower()
    if not any(marker in filename for marker in ("ci", "fixture", "test")):
        raise ValueError(
            "Synthetic fixture path must contain 'ci', 'fixture', or 'test'"
        )


def build_fixture(database_path: Path) -> None:
    """Create battery, hybrid, and hydrogen examples without raw identifiers."""
    _assert_fixture_path(database_path)
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(database_path))
    try:
        initialize_metadata(connection)
        initialize_model_tables(connection)
        connection.execute("BEGIN TRANSACTION")
        try:
            clear_staging(connection)
            drop_known_analytical_relations(connection)
            connection.execute(
                "DELETE FROM meta.ingestion_runs WHERE ingestion_id = ?",
                [FIXTURE_INGESTION_ID],
            )
            vehicles = normalize_vehicle_rows(
                [
                    {
                        "kenteken": "TEST_VEHICLE_BATTERY",
                        "merk": "Fictional Motors",
                        "handelsbenaming": "Battery One",
                        "datum_eerste_toelating": "20200115",
                        "eerste_kleur": "Blauw",
                        "voertuigsoort": "Personenauto",
                    },
                    {
                        "kenteken": "TEST_VEHICLE_HYBRID",
                        "merk": "Example Automotive",
                        "handelsbenaming": "Hybrid Two",
                        "datum_eerste_toelating": "20220601",
                        "eerste_kleur": "Groen",
                        "tweede_kleur": "",
                        "voertuigsoort": "Personenauto",
                    },
                    {
                        "kenteken": "TEST_VEHICLE_HYBRID_DIESEL",
                        "merk": "Imaginary Transport",
                        "handelsbenaming": "Hybrid Diesel Three",
                        "datum_eerste_toelating": "20230310",
                        "voertuigsoort": "Personenauto",
                    },
                    {
                        "kenteken": "TEST_VEHICLE_HYDROGEN",
                        "merk": "Synthetic Mobility",
                        "handelsbenaming": "Hydrogen Four",
                        "datum_eerste_toelating": "",
                        "eerste_kleur": "",
                        "voertuigsoort": "Bedrijfsauto",
                    },
                    {
                        "kenteken": "TEST_VEHICLE_HYDROGEN_MIXED",
                        "merk": "Mock Mobility",
                        "handelsbenaming": "Hydrogen Mixed Five",
                        "datum_eerste_toelating": "20240120",
                        "voertuigsoort": "Personenauto",
                    },
                    {
                        "kenteken": "TEST_VEHICLE_UNEXPECTED_FUEL",
                        "merk": "Sample Vehicles",
                        "handelsbenaming": "Alternative Six",
                        "datum_eerste_toelating": "20250201",
                        "voertuigsoort": "Personenauto",
                    },
                ],
                FIXTURE_SALT,
                FIXTURE_INGESTION_ID,
            )
            fuels = normalize_fuel_rows(
                [
                    {
                        "kenteken": "TEST_VEHICLE_BATTERY",
                        "brandstof_volgnummer": "1",
                        "brandstof_omschrijving": "Elektriciteit",
                        "nettomaximumvermogen": "150",
                    },
                    {
                        "kenteken": "TEST_VEHICLE_HYBRID",
                        "brandstof_volgnummer": "1",
                        "brandstof_omschrijving": "Elektriciteit",
                        "nettomaximumvermogen": "90",
                    },
                    {
                        "kenteken": "TEST_VEHICLE_HYBRID",
                        "brandstof_volgnummer": "2",
                        "brandstof_omschrijving": "Benzine",
                        "co2_uitstoot_gecombineerd": "42",
                    },
                    {
                        "kenteken": "TEST_VEHICLE_HYBRID_DIESEL",
                        "brandstof_volgnummer": "1",
                        "brandstof_omschrijving": "Elektriciteit",
                    },
                    {
                        "kenteken": "TEST_VEHICLE_HYBRID_DIESEL",
                        "brandstof_volgnummer": "2",
                        "brandstof_omschrijving": "Diesel",
                        "co2_uitstoot_gecombineerd": "55",
                    },
                    {
                        "kenteken": "TEST_VEHICLE_HYDROGEN",
                        "brandstof_volgnummer": "1",
                        "brandstof_omschrijving": "Waterstof",
                    },
                    {
                        "kenteken": "TEST_VEHICLE_HYDROGEN_MIXED",
                        "brandstof_volgnummer": "1",
                        "brandstof_omschrijving": "Waterstof",
                    },
                    {
                        "kenteken": "TEST_VEHICLE_HYDROGEN_MIXED",
                        "brandstof_volgnummer": "2",
                        "brandstof_omschrijving": "Benzine",
                    },
                    {
                        "kenteken": "TEST_VEHICLE_UNEXPECTED_FUEL",
                        "brandstof_volgnummer": "1",
                        "brandstof_omschrijving": "Elektriciteit",
                    },
                    {
                        "kenteken": "TEST_VEHICLE_UNEXPECTED_FUEL",
                        "brandstof_volgnummer": "2",
                        "brandstof_omschrijving": "LPG",
                    },
                ],
                FIXTURE_SALT,
                FIXTURE_INGESTION_ID,
            )
            upsert_staging_page(connection, vehicles, fuels)
            started_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
            connection.execute(
                """
                INSERT INTO meta.ingestion_runs (
                    ingestion_id, started_at, completed_at, status, sample_limit,
                    vehicle_rows, fuel_rows, duplicate_payloads, error_message,
                    ingestion_mode, requested_limit, page_size, pages_requested,
                    source_rows_received, matched_vehicles, rejected_rows,
                    active_duration_seconds, wall_clock_elapsed_seconds,
                    processed_rows_per_second, checkpoint_status, resumed,
                    resume_count
                )
                VALUES (?, ?, ?, 'succeeded', 6, 6, 10, 0, NULL,
                        'synthetic_ci_fixture', 6, 2, 8, 22, 6, 0,
                        1.0, 1.0, 16.0, 'completed', false, 0)
                """,
                [FIXTURE_INGESTION_ID, started_at, started_at],
            )
            connection.execute("COMMIT")
        except BaseException:
            connection.execute("ROLLBACK")
            raise
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build the explicitly synthetic DuckDB fixture used by CI"
    )
    parser.add_argument("--database", required=True, type=Path)
    args = parser.parse_args()
    build_fixture(args.database)


if __name__ == "__main__":
    main()
