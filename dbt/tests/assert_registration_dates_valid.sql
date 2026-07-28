select
    registration_date
from {{ ref('dim_registration_date') }}
where registration_date < date '1900-01-01'
   or registration_date > current_date + interval 1 year
