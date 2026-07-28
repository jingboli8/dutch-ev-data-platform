select
    table_schema,
    table_name,
    column_name
from information_schema.columns
where table_schema in ('staging', 'dbt_staging', 'intermediate', 'analytics')
  and lower(column_name) in (
      'kenteken',
      'licence_plate',
      'license_plate',
      'vehicle_identifier'
  )
