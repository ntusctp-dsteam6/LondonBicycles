from dagster import job
from orchestration.assets import extract_raw_data, ge_validate_cycle_hire_raw, dbt_transform, ge_validate_stg_cycle_hire, analytics_table

@job
def london_bicycles_job():
    extract_raw_data()
    ge_validate_cycle_hire_raw()
    dbt_transform()
    ge_validate_stg_cycle_hire()
    analytics_table()
