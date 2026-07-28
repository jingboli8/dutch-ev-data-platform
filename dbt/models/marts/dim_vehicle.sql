select
    vehicle_id_hash as vehicle_key,
    primary_colour,
    secondary_colour
from {{ ref('stg_vehicles') }}
