select
    md5(
        f.ingestion_id
        || '|' || f.vehicle_id_hash
        || '|' || cast(f.fuel_sequence as varchar)
    ) as vehicle_fuel_fact_key,
    s.vehicle_snapshot_key,
    f.vehicle_id_hash as vehicle_key,
    f.ingestion_id as snapshot_ingestion_id,
    f.fuel_sequence,
    f.fuel_type,
    f.emission_code,
    f.co2_combined_g_km,
    f.net_max_power_kw,
    f.hybrid_class
from {{ ref('stg_fuels') }} as f
inner join {{ ref('fact_vehicle_snapshot') }} as s
    on f.vehicle_id_hash = s.vehicle_key
    and f.ingestion_id = s.snapshot_ingestion_id
