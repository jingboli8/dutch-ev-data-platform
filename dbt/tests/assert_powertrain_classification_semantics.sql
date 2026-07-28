select
    vehicle_id_hash,
    powertrain_category,
    has_electric,
    has_hydrogen,
    has_other_fuel
from {{ ref('int_vehicle_fuel_profile') }}
where powertrain_category
    <> case
        when has_hydrogen then 'Hydrogen electric'
        when has_electric and not has_other_fuel then 'Battery electric'
        when has_electric and has_other_fuel then 'Hybrid electric'
    end
