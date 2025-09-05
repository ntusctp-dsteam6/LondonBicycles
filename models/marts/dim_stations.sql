SELECT
  station_id,
  station_name,
  latitude,
  longitude,
  installed,
  locked,
  install_date,
  removal_date,
  temporary
FROM {{ ref('stg_cycle_stations') }}