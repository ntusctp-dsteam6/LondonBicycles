SELECT
  f.rental_id,
  DATE(f.start_date) AS trip_start,
  DATE(f.end_date) AS trip_end,
  f.start_date AS trip_start_ts,
  f.end_date AS trip_end_ts,
  f.duration,
  f.bike_id,
  f.start_station_id,
  s_start.station_name AS start_station_name,
  REGEXP_EXTRACT(s_start.station_name, r',\s*(.*)$') AS start_station_area,
  f.end_station_id,
  s_end.station_name AS end_station_name,
  REGEXP_EXTRACT(s_end.station_name, r',\s*(.*)$') AS end_station_area
FROM {{ ref('stg_cycle_hire') }} f
LEFT JOIN {{ ref('dim_stations') }} s_start
  ON f.start_station_id = s_start.station_id
LEFT JOIN {{ ref('dim_stations') }} s_end
  ON f.end_station_id = s_end.station_id