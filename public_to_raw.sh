#!/bin/bash
# run_pipeline.sh (OAuth Version)
# This script orchestrates the entire pipeline:
# 1. Loads settings from the .env file.
# 2. Uses your personal 'gcloud' login to copy data from EU to US via GCS.
# 3. Triggers 'dbt' to transform the data into a Star Schema.

set -e # Exit the script immediately if any command fails

# --- Load Environment Variables from .env file ---
# --- Load Environment Variables from .env file ---
if [ -f .env ]; then
  echo "INFO: Loading settings from .env file..."
  # This loop reads the .env file line by line, making it much more robust.
  # It properly handles spaces, ignores comments, and skips blank lines.
  while IFS= read -r line || [[ -n "$line" ]]; do
      # Remove leading/trailing whitespace from the line
      line=$(echo "$line" | sed 's/^[ \t]*//;s/[ \t]*$//')

      # Skip comments and empty lines
      if [[ "$line" =~ ^# ]] || [[ -z "$line" ]]; then
          continue
      fi
      export "$line"
  done < .env
else
  echo "ERROR: .env file not found. Please create it with your project settings."
  exit 1
fi
# --- Global Configuration ---
export GOOGLE_PROJECT=$DSAI_PROJECT_ID
GCS_BUCKET_FULL_PATH="gs://$GCS_BUCKET_NAME"

# Source (EU) & Destination (US) variables
SOURCE_LOCATION="EU"
DESTINATION_LOCATION="US"
RAW_DATASET="raw_data" # A new dataset we will create to hold raw copies

# Table 1: cycle_hire
SOURCE_TABLE_HIRE="bigquery-public-data:london_bicycles.cycle_hire"
RAW_TABLE_HIRE="cycle_hire_raw"

# Table 2: cycle_stations
SOURCE_TABLE_STATIONS="bigquery-public-data:london_bicycles.cycle_stations"
RAW_TABLE_STATIONS="cycle_stations_raw"

# --- Pipeline Execution Starts Here ---
echo "INFO: ========================================================="
echo "INFO: STARTING LONDON BIKES PIPELINE | Project: $GOOGLE_PROJECT"

#echo "INFO: Step 1/7: Verifying DBT connection using your OAuth login..."
#dbt debug

echo "INFO: Step 2/7: Creating raw dataset '$RAW_DATASET' in US project..."
# This command creates a new dataset to store our raw copied data.
# The '|| true' part means 'if it fails (because it already exists), that's okay'.
bq --location=$DESTINATION_LOCATION mk --dataset $GOOGLE_PROJECT:$RAW_DATASET || true

echo "INFO: Step 3/7: Extracting EU tables to GCS..."
echo "  -> Extracting $SOURCE_TABLE_HIRE"
bq extract --location=$SOURCE_LOCATION --destination_format=AVRO "$SOURCE_TABLE_HIRE" "$GCS_BUCKET_FULL_PATH/export/$RAW_TABLE_HIRE/data-*"
echo "  -> Extracting $SOURCE_TABLE_STATIONS"
bq extract --location=$SOURCE_LOCATION --destination_format=AVRO "$SOURCE_TABLE_STATIONS" "$GCS_BUCKET_FULL_PATH/export/$RAW_TABLE_STATIONS/data-*"

echo "INFO: Step 4/7: Loading tables from GCS into US raw dataset..."
echo "  -> Loading $RAW_TABLE_HIRE"
bq load --location=$DESTINATION_LOCATION --source_format=AVRO --replace=true "$GOOGLE_PROJECT:$RAW_DATASET.$RAW_TABLE_HIRE" "$GCS_BUCKET_FULL_PATH/export/$RAW_TABLE_HIRE/data-*"
echo "  -> Loading $RAW_TABLE_STATIONS"
bq load --location=$DESTINATION_LOCATION --source_format=AVRO --replace=true "$GOOGLE_PROJECT:$RAW_DATASET.$RAW_TABLE_STATIONS" "$GCS_BUCKET_FULL_PATH/export/$RAW_TABLE_STATIONS/data-*"

echo "INFO: Step 5/7: Cleaning up temporary files from GCS bucket..."
gsutil -m rm "$GCS_BUCKET_FULL_PATH/export/**"

