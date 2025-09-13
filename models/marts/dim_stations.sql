SELECT
  station_id,
  station_name,
  latitude,
  longitude,
  installed,
  locked,
  install_date,
  removal_date,
  temporary,
  docks_count
FROM {{ ref('stg_cycle_stations') }}