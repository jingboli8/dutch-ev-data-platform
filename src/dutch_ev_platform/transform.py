"""Type-safe normalization, DuckDB modelling, and data-quality checks."""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

import duckdb

from .storage import hash_vehicle_id


class DataQualityError(RuntimeError):
    """Raised when required data-quality rules fail."""


REQUIRED_STAGING_SCHEMA = {
    "vehicles": {
        "vehicle_id_hash": {"VARCHAR"},
        "brand": {"VARCHAR"},
        "model": {"VARCHAR"},
        "registration_date": {"DATE"},
        "registration_year": {
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "HUGEINT",
        },
        "primary_colour": {"VARCHAR"},
        "secondary_colour": {"VARCHAR"},
        "vehicle_type": {"VARCHAR"},
        "ingestion_id": {"VARCHAR"},
    },
    "fuels": {
        "vehicle_id_hash": {"VARCHAR"},
        "fuel_sequence": {
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "HUGEINT",
        },
        "fuel_type": {"VARCHAR"},
        "emission_code": {"VARCHAR"},
        "co2_combined_g_km": {
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "HUGEINT",
            "FLOAT",
            "DOUBLE",
            "DECIMAL",
        },
        "net_max_power_kw": {
            "TINYINT",
            "SMALLINT",
            "INTEGER",
            "BIGINT",
            "HUGEINT",
            "FLOAT",
            "DOUBLE",
            "DECIMAL",
        },
        "hybrid_class": {"VARCHAR"},
        "ingestion_id": {"VARCHAR"},
    },
}


def _duckdb_base_type(value: str) -> str:
    return value.split("(", 1)[0].upper()


def validate_staging_schema(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    """Reject a missing or incompatible staging layer before any migration write."""
    problems: list[str] = []
    for table, expected_columns in REQUIRED_STAGING_SCHEMA.items():
        exists = connection.execute(
            """
            SELECT count(*)
            FROM information_schema.tables
            WHERE table_schema = 'staging' AND table_name = ?
            """,
            [table],
        ).fetchone()[0]
        if not exists:
            problems.append(f"missing staging.{table}")
            continue
        actual_columns = {
            row[1]: _duckdb_base_type(row[2])
            for row in connection.execute(
                f"PRAGMA table_info('staging.{table}')"
            ).fetchall()
        }
        for column, allowed_types in expected_columns.items():
            actual_type = actual_columns.get(column)
            if actual_type is None:
                problems.append(f"missing staging.{table}.{column}")
            elif actual_type not in allowed_types:
                problems.append(
                    f"incompatible staging.{table}.{column} type "
                    f"{actual_type}"
                )
    if problems:
        raise DataQualityError(
            "Transform-only staging schema is incompatible: "
            + "; ".join(problems)
        )


def parse_rdw_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    if len(text) != 8 or not text.isdigit():
        return None
    try:
        return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    except ValueError:
        return None


def parse_decimal(value: Any) -> Decimal | None:
    text = str(value or "").strip().replace(",", ".")
    if not text:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def parse_integer(value: Any) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def normalize_vehicle_rows(
    rows: list[dict[str, Any]], salt: str, ingestion_id: str
) -> list[dict[str, Any]]:
    normalized = []
    for row in rows:
        identifier = str(row.get("kenteken", "")).strip()
        if not identifier:
            continue
        registration_date = parse_rdw_date(row.get("datum_eerste_toelating"))
        normalized.append(
            {
                "vehicle_id_hash": hash_vehicle_id(identifier, salt),
                "brand": str(row.get("merk", "")).strip().upper() or None,
                "model": str(row.get("handelsbenaming", "")).strip().upper() or None,
                "registration_date": registration_date,
                "registration_year": registration_date.year if registration_date else None,
                "primary_colour": str(row.get("eerste_kleur", "")).strip().upper() or None,
                "secondary_colour": str(row.get("tweede_kleur", "")).strip().upper() or None,
                "vehicle_type": str(row.get("voertuigsoort", "")).strip() or None,
                "ingestion_id": ingestion_id,
            }
        )
    return normalized


def normalize_fuel_rows(
    rows: list[dict[str, Any]], salt: str, ingestion_id: str
) -> list[dict[str, Any]]:
    normalized = []
    for row in rows:
        identifier = str(row.get("kenteken", "")).strip()
        if not identifier:
            continue
        normalized.append(
            {
                "vehicle_id_hash": hash_vehicle_id(identifier, salt),
                "fuel_sequence": parse_integer(row.get("brandstof_volgnummer")) or 1,
                "fuel_type": str(row.get("brandstof_omschrijving", "")).strip() or "Unknown",
                "emission_code": str(row.get("emissiecode_omschrijving", "")).strip() or None,
                "co2_combined_g_km": parse_decimal(row.get("co2_uitstoot_gecombineerd")),
                "net_max_power_kw": parse_decimal(row.get("nettomaximumvermogen")),
                "hybrid_class": (
                    str(row.get("klasse_hybride_elektrisch_voertuig", "")).strip() or None
                ),
                "ingestion_id": ingestion_id,
            }
        )
    return normalized


def _register_rows(
    connection: duckdb.DuckDBPyConnection, view_name: str, rows: list[dict[str, Any]]
) -> None:
    import pandas as pd

    frame = pd.DataFrame(rows)
    connection.register(view_name, frame)


def initialize_model_tables(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("CREATE SCHEMA IF NOT EXISTS staging")
    connection.execute("CREATE SCHEMA IF NOT EXISTS analytics")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS staging.vehicles (
            vehicle_id_hash VARCHAR,
            brand VARCHAR,
            model VARCHAR,
            registration_date DATE,
            registration_year INTEGER,
            primary_colour VARCHAR,
            secondary_colour VARCHAR,
            vehicle_type VARCHAR,
            ingestion_id VARCHAR
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS staging.fuels (
            vehicle_id_hash VARCHAR,
            fuel_sequence INTEGER,
            fuel_type VARCHAR,
            emission_code VARCHAR,
            co2_combined_g_km DOUBLE,
            net_max_power_kw DOUBLE,
            hybrid_class VARCHAR,
            ingestion_id VARCHAR
        )
        """
    )
    # Migrate databases created by the early MVP prototype, whose all-null
    # hybrid field could have been inferred as INTEGER by pandas.
    connection.execute(
        """
        ALTER TABLE staging.fuels
        ALTER COLUMN hybrid_class SET DATA TYPE VARCHAR
        """
    )


def clear_staging(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("DELETE FROM staging.fuels")
    connection.execute("DELETE FROM staging.vehicles")


def upsert_staging_page(
    connection: duckdb.DuckDBPyConnection,
    vehicles: list[dict[str, Any]],
    fuels: list[dict[str, Any]],
) -> None:
    if not vehicles:
        raise DataQualityError("No valid vehicle rows remained after normalization")
    _register_rows(connection, "incoming_vehicles", vehicles)
    connection.execute(
        """
        DELETE FROM staging.vehicles
        USING incoming_vehicles i
        WHERE staging.vehicles.vehicle_id_hash = i.vehicle_id_hash
        """
    )
    connection.execute("INSERT INTO staging.vehicles SELECT * FROM incoming_vehicles")
    if fuels:
        _register_rows(connection, "incoming_fuels", fuels)
        connection.execute(
            """
            DELETE FROM staging.fuels
            USING incoming_fuels i
            WHERE staging.fuels.vehicle_id_hash = i.vehicle_id_hash
              AND staging.fuels.fuel_sequence = i.fuel_sequence
            """
        )
        connection.execute("INSERT INTO staging.fuels SELECT * FROM incoming_fuels")


def drop_known_analytical_relations(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    """Remove Phase 1 and dbt-owned outputs without touching user relations."""
    known_relations = {
        "dbt_staging": {
            "stg_vehicles",
            "stg_fuels",
            "stg_ingestion_runs",
        },
        "intermediate": {
            "int_vehicle_fuel_profile",
            "int_snapshot_context",
        },
        "analytics": {
            "ev_vehicles",
            "ev_fuel_details",
            "ev_metrics",
            "dim_vehicle",
            "dim_vehicle_model",
            "dim_registration_date",
            "dim_powertrain",
            "fact_vehicle_snapshot",
            "fact_vehicle_fuel",
            "mart_ev_overview",
            "mart_ev_metrics",
        },
    }
    rows = connection.execute(
        """
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema IN ('dbt_staging', 'intermediate', 'analytics')
        """
    ).fetchall()
    for schema, relation, relation_type in rows:
        if relation not in known_relations.get(schema, set()):
            continue
        object_type = "VIEW" if relation_type == "VIEW" else "TABLE"
        connection.execute(
            f'DROP {object_type} IF EXISTS "{schema}"."{relation}"'
        )


def run_staging_quality_checks(
    connection: duckdb.DuckDBPyConnection,
) -> dict[str, int]:
    vehicle_count = connection.execute(
        "SELECT count(*) FROM staging.vehicles"
    ).fetchone()[0]
    checks = {
        "null_vehicle_hashes": connection.execute(
            "SELECT count(*) FROM staging.vehicles WHERE vehicle_id_hash IS NULL"
        ).fetchone()[0],
        "duplicate_vehicles": connection.execute(
            """
            SELECT count(*) FROM (
                SELECT vehicle_id_hash FROM staging.vehicles
                GROUP BY vehicle_id_hash HAVING count(*) > 1
            )
            """
        ).fetchone()[0],
        "orphan_fuels": connection.execute(
            """
            SELECT count(*) FROM staging.fuels f
            LEFT JOIN staging.vehicles v USING (vehicle_id_hash)
            WHERE v.vehicle_id_hash IS NULL
            """
        ).fetchone()[0],
        "plain_identifier_columns": connection.execute(
            """
            SELECT count(*) FROM information_schema.columns
            WHERE table_schema IN ('staging', 'analytics')
              AND lower(column_name) IN ('kenteken', 'licence_plate', 'license_plate')
            """
        ).fetchone()[0],
        "invalid_vehicle_hashes": connection.execute(
            """
            SELECT count(*) FROM staging.vehicles
            WHERE length(vehicle_id_hash) != 64
               OR vehicle_id_hash !~ '^[0-9a-f]{64}$'
            """
        ).fetchone()[0],
        "invalid_registration_years": connection.execute(
            """
            SELECT count(*) FROM staging.vehicles
            WHERE registration_year IS NOT NULL
              AND (registration_year < 1900 OR registration_year > year(current_date) + 1)
            """
        ).fetchone()[0],
        "empty_vehicle_staging": int(vehicle_count == 0),
    }
    failures = {name: count for name, count in checks.items() if count}
    if failures:
        raise DataQualityError(f"Data-quality checks failed: {failures}")
    return checks
