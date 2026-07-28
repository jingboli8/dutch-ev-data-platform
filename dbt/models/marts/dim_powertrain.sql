select distinct
    md5(
        powertrain_category
        || '|' || cast(has_electric as varchar)
        || '|' || cast(has_hydrogen as varchar)
        || '|' || cast(has_other_fuel as varchar)
    ) as powertrain_key,
    powertrain_category,
    has_electric,
    has_hydrogen,
    has_other_fuel
from {{ ref('int_vehicle_fuel_profile') }}
