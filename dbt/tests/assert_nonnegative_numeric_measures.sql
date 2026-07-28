select 'fact_vehicle_snapshot.fuel_record_count' as failing_measure
from {{ ref('fact_vehicle_snapshot') }}
where fuel_record_count < 1

union all

select 'fact_vehicle_fuel.co2_combined_g_km' as failing_measure
from {{ ref('fact_vehicle_fuel') }}
where co2_combined_g_km < 0

union all

select 'fact_vehicle_fuel.net_max_power_kw' as failing_measure
from {{ ref('fact_vehicle_fuel') }}
where net_max_power_kw < 0

union all

select 'mart_ev_metrics.vehicle_count' as failing_measure
from {{ ref('mart_ev_metrics') }}
where vehicle_count < 1
