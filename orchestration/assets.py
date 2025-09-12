from dagster import asset
import subprocess

@asset
def extract_raw_data(context):
    script_path = "./public_to_raw.sh"
    result = subprocess.run(["bash", script_path], capture_output=True, text=True)

    context.log.info("Script stdout:\n" + result.stdout)

    if result.returncode != 0:
        context.log.error("Script stderr:\n" + result.stderr)
        raise Exception(f"Shell script failed with return code {result.returncode}:\n{result.stderr}")

    context.log.info("Raw data extracted successfully to GCS.")
    return "Raw data extracted to GCS."   # ✅ no Output()


@asset(deps=[extract_raw_data])
def dbt_transform(context):
    result = subprocess.run(["dbt", "run"], capture_output=True, text=True)

    context.log.info("DBT stdout:\n" + result.stdout)

    if result.returncode != 0:
        context.log.error("DBT stderr:\n" + result.stderr)
        raise Exception(f"DBT failed: {result.stderr}")

    return "DBT models built."   # ✅ plain value


@asset(deps=[dbt_transform])
def analytics_table(context):
    result = subprocess.run(["python", "notebooks/business_priya_2.1.py"], capture_output=True, text=True)

    context.log.info(result.stdout)

    if result.returncode != 0:
        raise Exception(result.stderr)

    return "Analytics table created."   # ✅ plain value
