select
    f.vehicle_snapshot_key,
    f.snapshot_ingestion_id,
    f.snapshot_started_at,
    v.vehicle_key,
    m.brand,
    m.model,
    m.vehicle_type,
    v.primary_colour,
    v.secondary_colour,
    d.registration_date,
    d.registration_year,
    p.powertrain_category,
    p.has_electric,
    p.has_hydrogen,
    p.has_other_fuel,
    f.fuel_record_count,
    f.vehicle_count
from {{ ref('fact_vehicle_snapshot') }} as f
inner join {{ ref('dim_vehicle') }} as v
    on f.vehicle_key = v.vehicle_key
inner join {{ ref('dim_vehicle_model') }} as m
    on f.vehicle_model_key = m.vehicle_model_key
inner join {{ ref('dim_powertrain') }} as p
    on f.powertrain_key = p.powertrain_key
left join {{ ref('dim_registration_date') }} as d
    on f.registration_date_key = d.registration_date_key
