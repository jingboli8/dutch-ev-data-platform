from __future__ import annotations

from dutch_ev_platform.transform import (
    normalize_fuel_rows,
    normalize_vehicle_rows,
    parse_rdw_date,
)


def test_vehicle_identifier_is_replaced_by_stable_hash(vehicle_rows):
    first = normalize_vehicle_rows(vehicle_rows, "salt", "run-1")
    second = normalize_vehicle_rows(vehicle_rows, "salt", "run-2")

    assert "kenteken" not in first[0]
    assert len(first[0]["vehicle_id_hash"]) == 64
    assert first[0]["vehicle_id_hash"] == second[0]["vehicle_id_hash"]
    assert first[0]["registration_year"] == 2020


def test_fuel_values_are_typed(fuel_rows):
    rows = normalize_fuel_rows(fuel_rows, "salt", "run-1")

    assert rows[0]["fuel_sequence"] == 1
    assert str(rows[0]["net_max_power_kw"]) == "153.00"
    assert rows[2]["co2_combined_g_km"] == 48


def test_invalid_rdw_date_becomes_null():
    assert parse_rdw_date("20231399") is None
    assert parse_rdw_date("") is None

