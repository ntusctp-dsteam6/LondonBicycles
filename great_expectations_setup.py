from great_expectations import get_context
from great_expectations.core.expectation_suite import ExpectationSuite
from google.cloud import bigquery

def get_valid_stations():
    """
    Query BigQuery to fetch distinct station_ids from stg_cycle_stations.
    """
    client = bigquery.Client()

    query = """
        SELECT DISTINCT station_id
        FROM `decisive-studio-469008-m2.LondonBicycles_Stage.stg_cycle_stations`
        WHERE station_id IS NOT NULL
    """

    query_job = client.query(query)
    results = query_job.result()

    valid_stations = [row.station_id for row in results]
    return valid_stations

# 1️⃣ Set GE root directory
gx_root_dir = "/home/jayaprakashn/LondonBicycles/great_expectations"
context = get_context(context_root_dir=gx_root_dir)
print("Great Expectations context initialized successfully!")

# 2️⃣ Get or create the BigQuery datasource
datasource_name = "london_bikes_bq"

try:
    datasource = context.get_datasource(datasource_name)
    print(f"Datasource '{datasource_name}' already exists, fetched from context.")
except Exception:
    datasource = context.sources.add_sql(
        name=datasource_name,
        connection_string="bigquery://decisive-studio-469008-m2"
    )
    print(f"Datasource '{datasource_name}' created!")

# 3️⃣ Ensure asset is registered
if "stg_cycle_hire" not in datasource.get_asset_names():
    datasource.add_query_asset(
        name="stg_cycle_hire",
        query="SELECT * FROM `decisive-studio-469008-m2.LondonBicycles_Stage.stg_cycle_hire`"
    )
    print("Asset 'stg_cycle_hire' registered under datasource!")

# 4️⃣ Build batch request from Fluent asset
asset = datasource.get_asset("stg_cycle_hire")
batch_request_hire = asset.build_batch_request()

# 5️⃣ Create or update an expectation suite
suite_name = "stg_cycle_hire_suite"
if suite_name not in [s.expectation_suite_name for s in context.list_expectation_suites()]:
    suite = ExpectationSuite(suite_name)
    context.add_or_update_expectation_suite(expectation_suite=suite)
print(f"Expectation suite '{suite_name}' ready!")

# 6️⃣ Create validator
validator = context.get_validator(
    batch_request=batch_request_hire,
    expectation_suite_name=suite_name
)
print("Validator ready!")

# 7️⃣ Add expectations
validator.expect_column_values_to_be_between("duration", min_value=0, max_value=86400,mostly=0.995)  # max 24 hours
validator.expect_column_values_to_not_be_null("start_station_id",mostly=0.98)

valid_stations = get_valid_stations()
validator.expect_column_values_to_be_in_set("start_station_id",value_set=valid_stations)
validator.expect_column_values_to_not_be_null("end_station_id",mostly=0.98)

# 8️⃣ Save the suite
validator.save_expectation_suite(discard_failed_expectations=False)
print("Expectation suite saved!")

# 9️⃣ Run validation via a checkpoint
checkpoint = context.add_or_update_checkpoint(
    name="stg_cycle_hire_checkpoint",
    validations=[
        {
            "batch_request": batch_request_hire,
            "expectation_suite_name": suite_name,
        }
    ],
)

result = checkpoint.run()
print("Validation complete! Results available in Data Docs.")

