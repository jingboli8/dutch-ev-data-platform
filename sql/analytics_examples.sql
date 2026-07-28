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

-- Incremental ingestion audit.
SELECT
    ingestion_id,
    started_at,
    completed_at,
    status,
    vehicle_rows,
    fuel_rows,
    duplicate_payloads
FROM meta.ingestion_runs
ORDER BY started_at DESC;

