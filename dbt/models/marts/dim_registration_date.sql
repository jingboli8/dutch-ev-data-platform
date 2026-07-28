select distinct
    cast(strftime(registration_date, '%Y%m%d') as integer) as registration_date_key,
    registration_date,
    year(registration_date) as registration_year,
    month(registration_date) as registration_month,
    quarter(registration_date) as registration_quarter
from {{ ref('stg_vehicles') }}
where registration_date is not null
