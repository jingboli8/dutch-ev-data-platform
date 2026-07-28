"""Verify dbt semantics against the explicitly synthetic CI fixture."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from dutch_ev_platform.storage import hash_vehicle_id
from scripts.build_ci_fixture import FIXTURE_SALT

EXPECTED_CATEGORIES = {
    "TEST_VEHICLE_BATTERY": "Battery electric",
    "TEST_VEHICLE_HYBRID": "Hybrid electric",
    "TEST_VEHICLE_HYBRID_DIESEL": "Hybrid electric",
    "TEST_VEHICLE_HYDROGEN": "Hydrogen electric",
    "TEST_VEHICLE_HYDROGEN_MIXED": "Hydrogen electric",
    "TEST_VEHICLE_UNEXPECTED_FUEL": "Hybrid electric",
}


def verify_fixture(database_path: Path) -> None:
    """Fail if classification, grains, or null metric semantics drift."""
    with duckdb.connect(str(database_path), read_only=True) as connection:
        actual = dict(
            connection.execute(
                """
                SELECT vehicle_key, powertrain_category
                FROM analytics.mart_ev_overview
                """
            ).fetchall()
        )
        expected = {
            hash_vehicle_id(identifier, FIXTURE_SALT): category
            for identifier, category in EXPECTED_CATEGORIES.items()
        }
        if actual != expected:
            raise RuntimeError(
                "Synthetic fixture powertrain classifications do not match "
                "the documented semantics"
            )

        counts = {
            table: connection.execute(
                f'SELECT count(*) FROM analytics."{table}"'
            ).fetchone()[0]
            for table in (
                "dim_vehicle",
                "fact_vehicle_snapshot",
                "fact_vehicle_fuel",
                "mart_ev_overview",
            )
        }
        if counts != {
            "dim_vehicle": 6,
            "fact_vehicle_snapshot": 6,
            "fact_vehicle_fuel": 10,
            "mart_ev_overview": 6,
        }:
            raise RuntimeError(
                f"Synthetic fixture model grains do not reconcile: {counts}"
            )

        null_metrics = connection.execute(
            """
            SELECT count(*)
            FROM analytics.fact_vehicle_fuel
            WHERE co2_combined_g_km IS NULL
              AND net_max_power_kw IS NULL
            """
        ).fetchone()[0]
        if null_metrics == 0:
            raise RuntimeError(
                "Synthetic missing optional metrics were converted to values"
            )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify dbt models built from the synthetic CI fixture"
    )
    parser.add_argument("--database", required=True, type=Path)
    args = parser.parse_args()
    verify_fixture(args.database)
    print("Synthetic dbt fixture semantics verified.")


if __name__ == "__main__":
    main()
