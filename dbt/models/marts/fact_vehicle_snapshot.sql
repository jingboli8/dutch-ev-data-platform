with vehicles as (
    select
        v.*,
        md5(
            coalesce(v.brand, '<unknown>')
            || '|' || coalesce(v.model, '<unknown>')
            || '|' || coalesce(v.vehicle_type, '<unknown>')
        ) as vehicle_model_key,
        case
            when v.registration_date is not null
                then cast(strftime(v.registration_date, '%Y%m%d') as integer)
        end as registration_date_key
    from {{ ref('stg_vehicles') }} as v
)

select
    md5(v.ingestion_id || '|' || v.vehicle_id_hash) as vehicle_snapshot_key,
    v.vehicle_id_hash as vehicle_key,
    v.vehicle_model_key,
    v.registration_date_key,
    md5(
        p.powertrain_category
        || '|' || cast(p.has_electric as varchar)
        || '|' || cast(p.has_hydrogen as varchar)
        || '|' || cast(p.has_other_fuel as varchar)
    ) as powertrain_key,
    v.ingestion_id as snapshot_ingestion_id,
    c.snapshot_started_at,
    p.fuel_record_count,
    1 as vehicle_count
from vehicles as v
inner join {{ ref('int_vehicle_fuel_profile') }} as p
    on v.vehicle_id_hash = p.vehicle_id_hash
left join {{ ref('int_snapshot_context') }} as c
    on v.ingestion_id = c.ingestion_id
