from dagster import asset
import subprocess
from great_expectations.data_context import DataContext

@asset
def extract_raw_data(context):
    script_path = "./public_to_raw.sh"
    result = subprocess.run(["bash", script_path], capture_output=True, text=True)

    context.log.info("Script stdout:\n" + result.stdout)

    if result.returncode > 2:
        context.log.error("Script stderr:\n" + result.stderr)
        raise Exception(f"Shell script failed with return code {result.returncode}:\n{result.stderr}")

    context.log.info("Raw data extracted successfully to GCS.")
    return "Raw data extracted to GCS."   # ✅ no Output()

# 🆕 New: Run Great Expectations after dbt
@asset(deps=[extract_raw_data])
def ge_validate_cycle_hire_raw(context):
    gx_root_dir = "/home/jayaprakashn/LondonBicycles/great_expectations"
    gx_context = DataContext(context_root_dir=gx_root_dir)

    checkpoint_name = "cycle_hire_raw_checkpoint"
    result = gx_context.run_checkpoint(checkpoint_name=checkpoint_name)

    if not result["success"]:
        raise Exception("Great Expectations validation failed 🚨")

    context.log.info("Great Expectations validation succeeded ✅")
    return "Validation passed."

@asset(deps=[extract_raw_data])
def dbt_transform(context):
    result = subprocess.run(["dbt", "run"], capture_output=True, text=True)

    context.log.info("DBT stdout:\n" + result.stdout)

    if result.returncode != 0:
        context.log.error("DBT stderr:\n" + result.stderr)
        raise Exception(f"DBT failed: {result.stderr}")

    return "DBT models built."   # ✅ plain value

# 🆕 New: Run Great Expectations after dbt
@asset(deps=[dbt_transform])
def ge_validate_stg_cycle_hire(context):
    gx_root_dir = "/home/jayaprakashn/LondonBicycles/great_expectations"
    gx_context = DataContext(context_root_dir=gx_root_dir)

    checkpoint_name = "stg_cycle_hire_checkpoint"
    result = gx_context.run_checkpoint(checkpoint_name=checkpoint_name)

    if not result["success"]:
        raise Exception("Great Expectations validation failed 🚨")

    context.log.info("Great Expectations validation succeeded ✅")
    return "Validation passed."

@asset(deps=[ge_validate_stg_cycle_hire])
def analytics_table(context):
    result = subprocess.run(["python", "notebooks/business_priya_2.2.py"], capture_output=True, text=True)

    context.log.info(result.stdout)

    if result.returncode != 0:
        raise Exception(result.stderr)

    return "Analytics table created."   # ✅ plain value
