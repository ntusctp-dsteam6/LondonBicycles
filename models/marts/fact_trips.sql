SELECT
  f.rental_id,
  f.start_date AS trip_start,
  f.end_date AS trip_end,
  f.duration,
  f.bike_id,
  f.start_station_id,
  s_start.station_name AS start_station_name,
  f.end_station_id,
  s_end.station_name AS end_station_name
FROM {{ ref('stg_cycle_hire') }} f
LEFT JOIN {{ ref('dim_stations') }} s_start
  ON f.start_station_id = s_start.station_id
LEFT JOIN {{ ref('dim_stations') }} s_end
  ON f.end_station_id = s_end.station_id