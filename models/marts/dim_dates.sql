with dates as (

  -- Start dates
  select
    DATE(start_date) AS date,
    EXTRACT(YEAR FROM start_date) AS year,
    EXTRACT(MONTH FROM start_date) AS month,
    EXTRACT(DAY FROM start_date) AS day,
    EXTRACT(DAYOFWEEK FROM start_date) AS weekday
  from {{ ref('stg_cycle_hire') }}

  union all

  -- End dates
  select
    DATE(end_date) AS date,
    EXTRACT(YEAR FROM end_date) AS year,
    EXTRACT(MONTH FROM end_date) AS month,
    EXTRACT(DAY FROM end_date) AS day,
    EXTRACT(DAYOFWEEK FROM end_date) AS weekday
  from {{ ref('stg_cycle_hire') }}

)

select distinct *
from dates
order by date