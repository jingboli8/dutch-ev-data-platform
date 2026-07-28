-- Top EV brands in the current dbt-owned snapshot mart.
SELECT
    brand,
    sum(vehicle_count) AS ev_vehicle_count
FROM analytics.mart_ev_overview
GROUP BY brand
ORDER BY ev_vehicle_count DESC, brand
LIMIT 10;

-- First-admission profile by year and powertrain category.
-- Registration is not interpreted as a vehicle sale.
SELECT
    registration_year,
    powertrain_category,
    sum(vehicle_count) AS vehicle_count
FROM analytics.mart_ev_overview
WHERE registration_year IS NOT NULL
GROUP BY registration_year, powertrain_category
ORDER BY registration_year DESC, vehicle_count DESC;

-- Current-snapshot aggregate by reported fuel and vehicle attributes.
SELECT
    fuel_type,
    brand,
    model,
    registration_year,
    powertrain_category,
    vehicle_count,
    avg_reported_co2_combined_g_km,
    avg_reported_net_max_power_kw
FROM analytics.mart_ev_metrics
ORDER BY vehicle_count DESC, brand, model
LIMIT 50;

-- Dimensional fact grain: one row per snapshot ingestion and vehicle.
SELECT
    snapshot_ingestion_id,
    count(*) AS vehicle_snapshot_rows,
    sum(fuel_record_count) AS reported_fuel_rows
FROM analytics.fact_vehicle_snapshot
GROUP BY snapshot_ingestion_id
ORDER BY snapshot_ingestion_id DESC;

-- Resumable snapshot ingestion audit (not a source-incremental load).
SELECT
    ingestion_id,
    ingestion_mode,
    requested_limit,
    started_at,
    completed_at,
    status,
    page_size,
    pages_requested,
    source_rows_received,
    vehicle_rows,
    fuel_rows,
    rejected_rows,
    duplicate_payloads,
    active_duration_seconds,
    wall_clock_elapsed_seconds,
    processed_rows_per_second,
    checkpoint_status,
    resumed,
    resume_count
FROM meta.ingestion_runs
ORDER BY started_at DESC;
