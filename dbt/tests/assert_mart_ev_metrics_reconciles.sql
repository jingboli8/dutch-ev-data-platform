with expected as (
    select
        ff.fuel_type,
        m.brand,
        m.model,
        d.registration_year,
        p.powertrain_category,
        count(distinct fs.vehicle_key) as vehicle_count,
        round(avg(ff.co2_combined_g_km), 2)
            as avg_reported_co2_combined_g_km,
        round(avg(ff.net_max_power_kw), 2)
            as avg_reported_net_max_power_kw
    from {{ ref('fact_vehicle_fuel') }} as ff
    inner join {{ ref('fact_vehicle_snapshot') }} as fs
        on ff.vehicle_snapshot_key = fs.vehicle_snapshot_key
    inner join {{ ref('dim_vehicle_model') }} as m
        on fs.vehicle_model_key = m.vehicle_model_key
    inner join {{ ref('dim_powertrain') }} as p
        on fs.powertrain_key = p.powertrain_key
    left join {{ ref('dim_registration_date') }} as d
        on fs.registration_date_key = d.registration_date_key
    group by
        ff.fuel_type,
        m.brand,
        m.model,
        d.registration_year,
        p.powertrain_category
),
differences as (
    (select * from expected except all select * from {{ ref('mart_ev_metrics') }})
    union all
    (select * from {{ ref('mart_ev_metrics') }} except all select * from expected)
)

select * from differences
