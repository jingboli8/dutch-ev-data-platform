select distinct
    md5(
        coalesce(brand, '<unknown>')
        || '|' || coalesce(model, '<unknown>')
        || '|' || coalesce(vehicle_type, '<unknown>')
    ) as vehicle_model_key,
    brand,
    model,
    vehicle_type
from {{ ref('stg_vehicles') }}
