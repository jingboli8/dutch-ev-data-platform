"""Type-safe normalization, DuckDB modelling, and data-quality checks."""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

import duckdb

from .storage import hash_vehicle_id


class DataQualityError(RuntimeError):
    """Raised when required data-quality rules fail."""


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


def build_models(
    connection: duckdb.DuckDBPyConnection,
    vehicles: list[dict[str, Any]],
    fuels: list[dict[str, Any]],
) -> None:
    if not vehicles:
        raise DataQualityError("No valid vehicle rows remained after normalization")
    _register_rows(connection, "incoming_vehicles", vehicles)
    _register_rows(connection, "incoming_fuels", fuels)
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
    connection.execute(
        """
        DELETE FROM staging.vehicles
        USING incoming_vehicles i
        WHERE staging.vehicles.vehicle_id_hash = i.vehicle_id_hash
        """
    )
    connection.execute("INSERT INTO staging.vehicles SELECT * FROM incoming_vehicles")
    if fuels:
        connection.execute(
            """
            DELETE FROM staging.fuels
            USING incoming_fuels i
            WHERE staging.fuels.vehicle_id_hash = i.vehicle_id_hash
              AND staging.fuels.fuel_sequence = i.fuel_sequence
            """
        )
        connection.execute("INSERT INTO staging.fuels SELECT * FROM incoming_fuels")
    connection.execute(
        """
        CREATE OR REPLACE TABLE analytics.ev_vehicles AS
        WITH fuel_profile AS (
            SELECT
                vehicle_id_hash,
                bool_or(lower(fuel_type) = 'elektriciteit') AS has_electric,
                bool_or(lower(fuel_type) = 'waterstof') AS has_hydrogen,
                count(*) AS fuel_count
            FROM staging.fuels
            GROUP BY vehicle_id_hash
        )
        SELECT
            v.vehicle_id_hash,
            v.brand,
            v.model,
            v.registration_date,
            v.registration_year,
            v.vehicle_type,
            CASE
                WHEN fp.has_hydrogen THEN 'Hydrogen electric'
                WHEN fp.has_electric AND fp.fuel_count = 1 THEN 'Battery electric'
                WHEN fp.has_electric THEN 'Plug-in or hybrid electric'
            END AS ev_category,
            v.ingestion_id
        FROM staging.vehicles v
        JOIN fuel_profile fp USING (vehicle_id_hash)
        WHERE fp.has_electric OR fp.has_hydrogen
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE TABLE analytics.ev_fuel_details AS
        SELECT
            ev.vehicle_id_hash,
            ev.brand,
            ev.model,
            ev.registration_year,
            ev.ev_category,
            f.fuel_type,
            f.co2_combined_g_km,
            f.net_max_power_kw
        FROM analytics.ev_vehicles ev
        JOIN staging.fuels f USING (vehicle_id_hash)
        """
    )
    connection.execute(
        """
        CREATE OR REPLACE TABLE analytics.ev_metrics AS
        SELECT
            fuel_type,
            brand,
            model,
            registration_year,
            count(DISTINCT vehicle_id_hash) AS vehicle_count,
            round(avg(co2_combined_g_km), 2) AS avg_co2_combined_g_km,
            round(avg(net_max_power_kw), 2) AS avg_net_max_power_kw
        FROM analytics.ev_fuel_details
        GROUP BY fuel_type, brand, model, registration_year
        """
    )


def run_quality_checks(connection: duckdb.DuckDBPyConnection) -> dict[str, int]:
    ev_row_count = connection.execute(
        "SELECT count(*) FROM analytics.ev_vehicles"
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
            WHERE table_schema = 'analytics'
              AND lower(column_name) IN ('kenteken', 'licence_plate', 'license_plate')
            """
        ).fetchone()[0],
        "invalid_registration_years": connection.execute(
            """
            SELECT count(*) FROM staging.vehicles
            WHERE registration_year IS NOT NULL
              AND (registration_year < 1900 OR registration_year > year(current_date) + 1)
            """
        ).fetchone()[0],
        "empty_ev_analytics": int(ev_row_count == 0),
    }
    failures = {name: count for name, count in checks.items() if count}
    if failures:
        raise DataQualityError(f"Data-quality checks failed: {failures}")
    return checks


def export_parquet(connection: duckdb.DuckDBPyConnection, parquet_dir: Any) -> None:
    parquet_dir.mkdir(parents=True, exist_ok=True)
    for schema, table in (
        ("staging", "vehicles"),
        ("staging", "fuels"),
        ("analytics", "ev_vehicles"),
        ("analytics", "ev_fuel_details"),
        ("analytics", "ev_metrics"),
    ):
        target = (parquet_dir / f"{schema}_{table}.parquet").as_posix()
        connection.execute(
            f"COPY {schema}.{table} TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
            [target],
        )
