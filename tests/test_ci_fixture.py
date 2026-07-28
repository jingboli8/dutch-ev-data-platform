from __future__ import annotations

import re

import duckdb

from scripts.build_ci_fixture import build_fixture


def test_synthetic_ci_fixture_is_privacy_safe_and_deterministic(tmp_path):
    database_path = tmp_path / "ci_fixture.duckdb"

    build_fixture(database_path)
    build_fixture(database_path)

    with duckdb.connect(str(database_path), read_only=True) as connection:
        vehicle_hashes = [
            row[0]
            for row in connection.execute(
                "SELECT vehicle_id_hash FROM staging.vehicles ORDER BY 1"
            ).fetchall()
        ]
        fuel_types = {
            row[0]
            for row in connection.execute(
                "SELECT DISTINCT fuel_type FROM staging.fuels"
            ).fetchall()
        }
        plaintext_columns = connection.execute(
            """
            SELECT count(*)
            FROM information_schema.columns
            WHERE table_schema = 'staging'
              AND lower(column_name) IN (
                  'kenteken', 'licence_plate', 'license_plate'
              )
            """
        ).fetchone()[0]

    assert len(vehicle_hashes) == 6
    assert all(re.fullmatch(r"[0-9a-f]{64}", value) for value in vehicle_hashes)
    assert fuel_types == {
        "Elektriciteit",
        "Benzine",
        "Diesel",
        "Waterstof",
        "LPG",
    }
    assert plaintext_columns == 0


def test_fixture_builder_rejects_a_production_like_path(tmp_path):
    database_path = tmp_path.parent / "warehouse.duckdb"

    try:
        build_fixture(database_path)
    except ValueError as exc:
        assert "Synthetic fixture path" in str(exc)
    else:
        raise AssertionError("fixture builder accepted a production-like path")
