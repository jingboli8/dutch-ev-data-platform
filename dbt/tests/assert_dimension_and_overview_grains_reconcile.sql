with checks as (
    select
        'dim_vehicle' as model_name,
        (select count(*) from {{ ref('stg_vehicles') }}) as expected_rows,
        (select count(*) from {{ ref('dim_vehicle') }}) as actual_rows

    union all

    select
        'dim_registration_date',
        (
            select count(*)
            from (
                select distinct registration_date
                from {{ ref('stg_vehicles') }}
                where registration_date is not null
            )
        ),
        (select count(*) from {{ ref('dim_registration_date') }})

    union all

    select
        'dim_vehicle_model',
        (
            select count(*)
            from (
                select distinct brand, model, vehicle_type
                from {{ ref('stg_vehicles') }}
            )
        ),
        (select count(*) from {{ ref('dim_vehicle_model') }})

    union all

    select
        'dim_powertrain',
        (
            select count(*)
            from (
                select distinct
                    powertrain_category,
                    has_electric,
                    has_hydrogen,
                    has_other_fuel
                from {{ ref('int_vehicle_fuel_profile') }}
            )
        ),
        (select count(*) from {{ ref('dim_powertrain') }})

    union all

    select
        'mart_ev_overview',
        (select count(*) from {{ ref('fact_vehicle_snapshot') }}),
        (select count(*) from {{ ref('mart_ev_overview') }})
)

select *
from checks
where expected_rows <> actual_rows
