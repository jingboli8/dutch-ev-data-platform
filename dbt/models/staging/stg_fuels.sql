select
    cast(vehicle_id_hash as varchar) as vehicle_id_hash,
    cast(fuel_sequence as integer) as fuel_sequence,
    nullif(trim(cast(fuel_type as varchar)), '') as fuel_type,
    nullif(trim(cast(emission_code as varchar)), '') as emission_code,
    cast(co2_combined_g_km as double) as co2_combined_g_km,
    cast(net_max_power_kw as double) as net_max_power_kw,
    nullif(trim(cast(hybrid_class as varchar)), '') as hybrid_class,
    cast(ingestion_id as varchar) as ingestion_id
from {{ source('python_staging', 'fuels') }}
