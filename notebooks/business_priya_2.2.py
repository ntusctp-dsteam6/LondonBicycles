# Import libraries
from google.cloud import bigquery
from google.cloud import bigquery_storage
from dotenv import load_dotenv
import os

# -----------------------------
# Setup
# -----------------------------
load_dotenv()  # Load environment variables
project_id = os.environ.get("DSAI_PROJECT_ID")
client = bigquery.Client(project=project_id)
bqstorage_client = bigquery_storage.BigQueryReadClient()
analytics_dataset = "LondonBicycles_Analytics"

# Duration filter for outliers
DURATION_MIN = 1      # minutes
DURATION_MAX = 240    # minutes
DURATION_SEC_MIN = DURATION_MIN * 60
DURATION_SEC_MAX = DURATION_MAX * 60

# -----------------------------
# 1. Daily summaries
# -----------------------------
query_daily = f"""
SELECT
  d.date,
  d.year,
  d.month,
  d.weekday,
  COUNT(t.rental_id) AS trip_count,
  ROUND(AVG(t.duration)/60, 2) AS avg_duration_minutes,
  ROUND(MIN(t.duration)/60, 2) AS min_duration_minutes,
  ROUND(MAX(t.duration)/60, 2) AS max_duration_minutes
FROM `{project_id}.LondonBicycles_Core.fact_trips` t
LEFT JOIN `{project_id}.LondonBicycles_Core.dim_dates` d
  ON t.trip_start = d.date
WHERE t.duration BETWEEN {DURATION_SEC_MIN} AND {DURATION_SEC_MAX}
GROUP BY d.date, d.year, d.month, d.weekday
ORDER BY d.date
"""
daily_table = f"{project_id}.{analytics_dataset}.daily_summaries"
client.query(query_daily, job_config=bigquery.QueryJobConfig(destination=daily_table, write_disposition="WRITE_TRUNCATE")).result()
print(f"✅ Daily summaries saved: {daily_table}")

# -----------------------------
# 2. Weekly summaries
# -----------------------------
query_weekly = f"""
SELECT
  d.year,
  EXTRACT(WEEK FROM d.date) AS week,
  COUNT(t.rental_id) AS trip_count,
  ROUND(AVG(t.duration)/60, 2) AS avg_duration_minutes,
  ROUND(MIN(t.duration)/60, 2) AS min_duration_minutes
FROM `{project_id}.LondonBicycles_Core.fact_trips` t
JOIN `{project_id}.LondonBicycles_Core.dim_dates` d
  ON t.trip_start = d.date
WHERE t.duration BETWEEN {DURATION_SEC_MIN} AND {DURATION_SEC_MAX}
GROUP BY d.year, week
ORDER BY d.year, week
"""
weekly_table = f"{project_id}.{analytics_dataset}.weekly_summaries"
client.query(query_weekly, job_config=bigquery.QueryJobConfig(destination=weekly_table, write_disposition="WRITE_TRUNCATE")).result()
print(f"✅ Weekly summaries saved: {weekly_table}")

# -----------------------------
# 3. Monthly summaries
# -----------------------------
query_monthly = f"""
SELECT
  d.year,
  d.month,
  COUNT(t.rental_id) AS trip_count,
  ROUND(AVG(t.duration)/60, 2) AS avg_duration_minutes,
  ROUND(MIN(t.duration)/60, 2) AS min_duration_minutes
FROM `{project_id}.LondonBicycles_Core.fact_trips` t
JOIN `{project_id}.LondonBicycles_Core.dim_dates` d
  ON t.trip_start = d.date
WHERE t.duration BETWEEN {DURATION_SEC_MIN} AND {DURATION_SEC_MAX}
GROUP BY d.year, d.month
ORDER BY d.year, d.month
"""
monthly_table = f"{project_id}.{analytics_dataset}.monthly_summaries"
client.query(query_monthly, job_config=bigquery.QueryJobConfig(destination=monthly_table, write_disposition="WRITE_TRUNCATE")).result()
print(f"✅ Monthly summaries saved: {monthly_table}")

# -----------------------------
# 4. Hourly counts
# -----------------------------
query_hourly = f"""
SELECT
  d.date,
  d.year,
  d.month,
  d.weekday,
  EXTRACT(HOUR FROM t.trip_start_ts) AS trip_hour,
  COUNT(t.rental_id) AS trip_count,
  ROUND(AVG(t.duration)/60, 2) AS avg_duration_minutes
FROM `{project_id}.LondonBicycles_Core.fact_trips` t
JOIN `{project_id}.LondonBicycles_Core.dim_dates` d
  ON t.trip_start = d.date
WHERE t.duration BETWEEN {DURATION_SEC_MIN} AND {DURATION_SEC_MAX}
GROUP BY d.date, d.year, d.month, d.weekday, trip_hour
ORDER BY d.date, trip_hour
"""
hourly_table = f"{project_id}.{analytics_dataset}.hourly_counts"
client.query(query_hourly, job_config=bigquery.QueryJobConfig(destination=hourly_table, write_disposition="WRITE_TRUNCATE")).result()
print(f"✅ Hourly counts saved: {hourly_table}")

# -----------------------------
# 5. Top stations
# -----------------------------
query_stations = f"""
SELECT
  s.station_id,
  s.station_name,
  TRIM(SPLIT(s.station_name, ',')[OFFSET(1)]) AS station_area,
  s.latitude,
  s.longitude,
  s.docks_count,
  d.year,
  d.month,
  COUNTIF(t.start_station_id = s.station_id) AS trips_started,
  COUNTIF(t.end_station_id = s.station_id) AS trips_ended,
  ROUND(AVG(CASE WHEN t.start_station_id = s.station_id THEN t.duration END)/60, 2) AS avg_duration_from_station,
  ROUND(AVG(CASE WHEN t.end_station_id = s.station_id THEN t.duration END)/60, 2) AS avg_duration_to_station
FROM `{project_id}.LondonBicycles_Core.fact_trips` t
JOIN `{project_id}.LondonBicycles_Core.dim_stations` s
  ON t.start_station_id = s.station_id OR t.end_station_id = s.station_id
JOIN `{project_id}.LondonBicycles_Core.dim_dates` d
  ON t.trip_start = d.date
WHERE t.duration BETWEEN {DURATION_SEC_MIN} AND {DURATION_SEC_MAX}
GROUP BY s.station_id, s.station_name, station_area, s.latitude, s.longitude, s.docks_count, d.year, d.month
ORDER BY d.year, d.month, GREATEST(trips_started, trips_ended) DESC
"""
stations_table = f"{project_id}.{analytics_dataset}.top_stations"
client.query(query_stations, job_config=bigquery.QueryJobConfig(destination=stations_table, write_disposition="WRITE_TRUNCATE")).result()
print(f"✅ Top stations saved: {stations_table}")

# -----------------------------
# 6. Route Popularity
# -----------------------------
query_route_popularity = f"""
SELECT
  d.year,
  d.month,
  d.day,
  EXTRACT(HOUR FROM t.trip_start_ts) AS trip_hour,
  t.start_station_id,
  t.start_station_name,
  TRIM(SPLIT(t.start_station_name, ',')[OFFSET(1)]) AS start_station_area,
  t.end_station_id,
  t.end_station_name,
  TRIM(SPLIT(t.end_station_name, ',')[OFFSET(1)]) AS end_station_area,
  COUNT(*) AS trip_count,
  ROUND(AVG(t.duration)/60, 2) AS avg_duration_minutes
FROM `{project_id}.LondonBicycles_Core.fact_trips` t
JOIN `{project_id}.LondonBicycles_Core.dim_dates` d
  ON t.trip_start = d.date
WHERE t.duration BETWEEN {DURATION_SEC_MIN} AND {DURATION_SEC_MAX}
AND t.trip_start >= DATE_SUB((SELECT MAX(trip_start) FROM `{project_id}.LondonBicycles_Core.fact_trips`), INTERVAL 12 MONTH)
GROUP BY d.year, d.month, d.day, trip_hour,
         t.start_station_id, t.start_station_name, start_station_area,
         t.end_station_id, t.end_station_name, end_station_area
ORDER BY d.year, d.month, trip_hour, trip_count DESC
"""
popularity_table = f"{project_id}.{analytics_dataset}.route_popularity"
client.query(query_route_popularity, job_config=bigquery.QueryJobConfig(destination=popularity_table, write_disposition="WRITE_TRUNCATE")).result()
print(f"✅ Route popularity saved: {popularity_table}")

# -----------------------------
# 7. Trip duration histogram
# -----------------------------
query_duration_hist = f"""
SELECT
  d.year,
  d.month,
  EXTRACT(HOUR FROM t.trip_start_ts) AS trip_hour,
  CAST(ROUND(t.duration/60,0) AS INT64) AS duration_minutes_bin,
  COUNT(*) AS trip_count
FROM `{project_id}.LondonBicycles_Core.fact_trips` t
JOIN `{project_id}.LondonBicycles_Core.dim_dates` d
  ON t.trip_start = d.date
WHERE t.duration BETWEEN {DURATION_SEC_MIN} AND {DURATION_SEC_MAX}
GROUP BY d.year, d.month, trip_hour, duration_minutes_bin
ORDER BY d.year, d.month, trip_hour, duration_minutes_bin
"""
duration_table = f"{project_id}.{analytics_dataset}.trip_duration_histogram"
client.query(query_duration_hist, job_config=bigquery.QueryJobConfig(destination=duration_table, write_disposition="WRITE_TRUNCATE")).result()
print(f"✅ Trip duration histogram saved: {duration_table}")

# -----------------------------
# 8. Duration Band Stats
# -----------------------------
query_duration_band = f"""
SELECT
  d.year,
  d.month,
  CASE
    WHEN t.duration < 300 THEN 'Under 5 min'
    WHEN t.duration BETWEEN 300 AND 900 THEN '5-15 min'
    WHEN t.duration BETWEEN 900 AND 1800 THEN '15-30 min'
    WHEN t.duration BETWEEN 1800 AND 2700 THEN '30-45 min'
    WHEN t.duration BETWEEN 2700 AND 3600 THEN '45-60 min'
    WHEN t.duration > 3600 THEN 'Over 60 min'
  END AS duration_band,
  COUNT(*) AS trip_count,
  ROUND(COUNT(*)*100.0/SUM(COUNT(*)) OVER (PARTITION BY d.year,d.month),2) AS pct_of_total,
  ROUND(AVG(t.duration)/60,2) AS avg_duration_minutes
FROM `{project_id}.LondonBicycles_Core.fact_trips` t
JOIN `{project_id}.LondonBicycles_Core.dim_dates` d
  ON t.trip_start = d.date
WHERE t.duration BETWEEN {DURATION_SEC_MIN} AND {DURATION_SEC_MAX}
GROUP BY d.year,d.month,duration_band
ORDER BY d.year,d.month,trip_count DESC
"""
duration_band_table = f"{project_id}.{analytics_dataset}.duration_band"
client.query(query_duration_band, job_config=bigquery.QueryJobConfig(destination=duration_band_table, write_disposition="WRITE_TRUNCATE")).result()
print(f"✅ Duration band stats saved: {duration_band_table}")

# -----------------------------
# 9. Return_to_origin Stats
# -----------------------------
query_return_origin = f"""
SELECT
  d.year,
  d.month,
  COUNTIF(t.start_station_id = t.end_station_id) AS same_station_trips,
  COUNT(*) AS total_trips,
  ROUND(COUNTIF(t.start_station_id = t.end_station_id)*100.0/COUNT(*),2) AS pct_same_station_trips
FROM `{project_id}.LondonBicycles_Core.fact_trips` t
JOIN `{project_id}.LondonBicycles_Core.dim_dates` d
  ON t.trip_start = d.date
WHERE t.duration BETWEEN {DURATION_SEC_MIN} AND {DURATION_SEC_MAX}
GROUP BY d.year, d.month
ORDER BY d.year, d.month
"""
return_origin_table = f"{project_id}.{analytics_dataset}.return_to_origin"
client.query(query_return_origin, job_config=bigquery.QueryJobConfig(destination=return_origin_table, write_disposition="WRITE_TRUNCATE")).result()
print(f"✅ Return to origin stats saved: {return_origin_table}")

# -----------------------------
# 10. Station demand supply gap
# -----------------------------
query_supply_demand = f"""
SELECT
  d.year,
  d.month,
  s.station_id,
  s.station_name,
  COUNTIF(t.start_station_id = s.station_id) AS trips_started,
  COUNTIF(t.end_station_id = s.station_id) AS trips_ended,
  (COUNTIF(t.end_station_id = s.station_id)-COUNTIF(t.start_station_id = s.station_id)) AS net_inflow,
  ROUND(SAFE_DIVIDE((COUNTIF(t.end_station_id = s.station_id)-COUNTIF(t.start_station_id = s.station_id)),NULLIF(s.docks_count,0)),2) AS inflow_ratio_per_dock
FROM `{project_id}.LondonBicycles_Core.fact_trips` t
JOIN `{project_id}.LondonBicycles_Core.dim_stations` s
  ON t.start_station_id = s.station_id OR t.end_station_id = s.station_id
JOIN `{project_id}.LondonBicycles_Core.dim_dates` d
  ON t.trip_start = d.date
WHERE t.duration BETWEEN {DURATION_SEC_MIN} AND {DURATION_SEC_MAX}
GROUP BY d.year,d.month,s.station_id,s.station_name,s.docks_count
ORDER BY ABS(net_inflow) DESC
"""
supply_demand_table = f"{project_id}.{analytics_dataset}.station_demand_supply_gap"
client.query(query_supply_demand, job_config=bigquery.QueryJobConfig(destination=supply_demand_table, write_disposition="WRITE_TRUNCATE")).result()
print(f"✅ Station demand supply gap saved: {supply_demand_table}")

# Analytical dataset and new table name
analytics_dataset = "LondonBicycles_Analytics"
station_static_table = f"{project_id}.{analytics_dataset}.station_static"

# Query to copy dim_stations into analytical layer
query_station_static = f"""
CREATE OR REPLACE TABLE `{station_static_table}` AS
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
FROM `{project_id}.LondonBicycles_Core.dim_stations`;
"""

# Run the query
client.query(query_station_static).result()
print(f"✅ Station static table created in analytics layer: {station_static_table}")
