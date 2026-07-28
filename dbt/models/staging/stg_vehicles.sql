select
    cast(vehicle_id_hash as varchar) as vehicle_id_hash,
    nullif(trim(cast(brand as varchar)), '') as brand,
    nullif(trim(cast(model as varchar)), '') as model,
    cast(registration_date as date) as registration_date,
    cast(registration_year as integer) as registration_year,
    nullif(trim(cast(primary_colour as varchar)), '') as primary_colour,
    nullif(trim(cast(secondary_colour as varchar)), '') as secondary_colour,
    nullif(trim(cast(vehicle_type as varchar)), '') as vehicle_type,
    cast(ingestion_id as varchar) as ingestion_id
from {{ source('python_staging', 'vehicles') }}
