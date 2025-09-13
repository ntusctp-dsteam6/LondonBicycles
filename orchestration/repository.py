from dagster import repository
from orchestration.jobs import london_bicycles_job
from orchestration.assets import extract_raw_data, ge_validate_cycle_hire_raw, dbt_transform, ge_validate_stg_cycle_hire, analytics_table
from orchestration.schedules import london_bicycles_schedule

@repository
def london_bicycles_repo():
    return [
        london_bicycles_job,
        extract_raw_data,
        ge_validate_cycle_hire_raw,
        dbt_transform,
        ge_validate_stg_cycle_hire,
        analytics_table,
        london_bicycles_schedule,
    ]
