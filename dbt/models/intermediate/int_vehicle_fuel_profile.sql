with fuel_profile as (
    select
        vehicle_id_hash,
        count(*) as fuel_record_count,
        bool_or(lower(fuel_type) = 'elektriciteit') as has_electric,
        bool_or(lower(fuel_type) = 'waterstof') as has_hydrogen,
        bool_or(lower(fuel_type) not in ('elektriciteit', 'waterstof')) as has_other_fuel
    from {{ ref('stg_fuels') }}
    group by vehicle_id_hash
)

select
    vehicle_id_hash,
    fuel_record_count,
    has_electric,
    has_hydrogen,
    has_other_fuel,
    case
        when has_hydrogen then 'Hydrogen electric'
        when has_electric and not has_other_fuel then 'Battery electric'
        when has_electric and has_other_fuel then 'Hybrid electric'
    end as powertrain_category
from fuel_profile
where has_electric or has_hydrogen
