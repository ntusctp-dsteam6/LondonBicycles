SELECT DISTINCT
  DATE(TIMESTAMP_MILLIS(start_date)) AS date,
  EXTRACT(YEAR FROM TIMESTAMP_MILLIS(start_date)) AS year,
  EXTRACT(MONTH FROM TIMESTAMP_MILLIS(start_date)) AS month,
  EXTRACT(DAY FROM TIMESTAMP_MILLIS(start_date)) AS day,
  EXTRACT(DAYOFWEEK FROM TIMESTAMP_MILLIS(start_date)) AS weekday
FROM {{ ref('stg_cycle_hire') }}
WHERE start_date > 0
  AND start_date < 253402300799000  -- max allowed TIMESTAMP_MILLIS in ms (~9999-12-31)