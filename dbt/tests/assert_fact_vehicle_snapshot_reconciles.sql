select
    (select count(*) from {{ ref('stg_vehicles') }}) as staging_rows,
    (select count(*) from {{ ref('fact_vehicle_snapshot') }}) as fact_rows
where staging_rows <> fact_rows
