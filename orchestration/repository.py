from dagster import repository
from orchestration.jobs import london_bicycles_job
from orchestration.assets import extract_raw_data, dbt_transform, analytics_table
from orchestration.schedules import london_bicycles_schedule

@repository
def london_bicycles_repo():
    return [
        london_bicycles_job,
        extract_raw_data,
        dbt_transform,
        analytics_table,
        london_bicycles_schedule,
    ]
