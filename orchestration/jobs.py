from dagster import job
from orchestration.assets import extract_raw_data, dbt_transform, analytics_table

@job
def london_bicycles_job():
    extract_raw_data()
    dbt_transform()
    analytics_table()
