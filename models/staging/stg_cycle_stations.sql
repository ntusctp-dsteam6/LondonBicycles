SELECT
  id AS station_id,
  name AS station_name,
  latitude,
  longitude,
  installed,
  locked,
  install_date,
  removal_date,
  temporary
FROM `decisive-studio-469008-m2.raw_data.cycle_stations_raw`