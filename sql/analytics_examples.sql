-- Top EV brands in the current local sample.
SELECT
    brand,
    count(*) AS ev_vehicle_count
FROM analytics.ev_vehicles
GROUP BY brand
ORDER BY ev_vehicle_count DESC, brand
LIMIT 10;

-- EV adoption profile by registration year and category.
SELECT
    registration_year,
    ev_category,
    count(*) AS vehicle_count
FROM analytics.ev_vehicles
WHERE registration_year IS NOT NULL
GROUP BY registration_year, ev_category
ORDER BY registration_year DESC, vehicle_count DESC;

-- Detailed aggregate requested by fuel, brand, model, and registration year.
SELECT
    fuel_type,
    brand,
    model,
    registration_year,
    vehicle_count,
    avg_co2_combined_g_km,
    avg_net_max_power_kw
FROM analytics.ev_metrics
ORDER BY vehicle_count DESC, brand, model
LIMIT 50;

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
