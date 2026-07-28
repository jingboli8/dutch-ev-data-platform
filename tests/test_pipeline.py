from __future__ import annotations

import duckdb

from dutch_ev_platform.pipeline import run_pipeline


class FakeClient:
    def __init__(self, vehicles, fuels):
        self.vehicles = vehicles
        self.fuels = fuels

    def fetch_ev_identifier_sample(self, limit):
        return ["TEST_VEHICLE_001", "TEST_VEHICLE_002"][:limit]

    def fetch_vehicles_by_ids(self, vehicle_ids):
        assert set(vehicle_ids) == {"TEST_VEHICLE_001", "TEST_VEHICLE_002"}
        return self.vehicles

    def fetch_fuels_for_vehicles(self, vehicle_ids):
        assert set(vehicle_ids) == {"TEST_VEHICLE_001", "TEST_VEHICLE_002"}
        return self.fuels


def test_pipeline_builds_private_analytical_layer(
    settings, vehicle_rows, fuel_rows
):
    result = run_pipeline(settings, FakeClient(vehicle_rows, fuel_rows))

    assert result["ev_rows"] == 2
    assert result["quality_checks"]["plain_identifier_columns"] == 0
    assert list(settings.raw_dir.rglob("*.json"))
    assert (settings.parquet_dir / "analytics_ev_metrics.parquet").exists()
    with duckdb.connect(str(settings.database_path), read_only=True) as connection:
        assert connection.execute(
            "SELECT count(*) FROM analytics.ev_metrics"
        ).fetchone()[0] == 3
        columns = {
            row[1]
            for row in connection.execute(
                "PRAGMA table_info('analytics.ev_vehicles')"
            ).fetchall()
        }
        assert "kenteken" not in columns


def test_repeated_payload_is_audited_without_duplicate_staging_rows(
    settings, vehicle_rows, fuel_rows
):
    client = FakeClient(vehicle_rows, fuel_rows)
    run_pipeline(settings, client)
    second = run_pipeline(settings, client)

    assert second["duplicate_payloads"] == 2
    assert len(list(settings.raw_dir.rglob("*.json"))) == 2
    with duckdb.connect(str(settings.database_path), read_only=True) as connection:
        assert connection.execute(
            "SELECT count(*) FROM staging.vehicles"
        ).fetchone()[0] == 2
