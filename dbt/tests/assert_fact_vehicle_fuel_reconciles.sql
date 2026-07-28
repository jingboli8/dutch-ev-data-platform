select
    (select count(*) from {{ ref('stg_fuels') }}) as staging_rows,
    (select count(*) from {{ ref('fact_vehicle_fuel') }}) as fact_rows
where staging_rows <> fact_rows
