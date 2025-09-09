# LondonBicycles
This is Team 6's project on the London Bicycle's dataset.

## Streamlit App (BigQuery Explorer)
Interactive dashboards to explore trips, stations, routes, and weekday patterns, backed by BigQuery tables built by this dbt project.

### App Location
- `apps/streamlit/streamlit_londonbikes_app.py`

### Prerequisites
- Python 3.9+
- Packages: `streamlit`, `google-cloud-bigquery`, `plotly`, `python-dotenv`, `pandas`
- GCP auth via either:
  - Service account JSON: set `GOOGLE_APPLICATION_CREDENTIALS=/abs/path/to/sa.json`, or
  - `gcloud auth application-default login` (ADC), or
  - Streamlit secrets (`.streamlit/secrets.toml` with `[gcp] project_id=... credentials=...`).

### Run Locally
```
pip install streamlit google-cloud-bigquery plotly python-dotenv pandas
export DSAI_PROJECT_ID=<your-gcp-project>
# one of the following auth options:
# export GOOGLE_APPLICATION_CREDENTIALS=/abs/path/to/sa.json
# or: gcloud auth application-default login
streamlit run apps/streamlit/streamlit_londonbikes_app.py
```

### Configuration
- Dataset: defaults to `LondonBicycles` (change in sidebar if needed)
- Date Range: defaults to 01/01/2021–31/12/2022; if no data found, use the sidebar button to switch to dataset min/max.

### Tabs & Charts
- Overview: KPIs, trips over time, top stations, duration distribution.
- Routes: Top start→end routes (bar), route map (Mapbox), area-to-area heatmap.
- Weekdays: Trips by weekday, top routes per weekday.
- Stations & Map: Bubble map of stations by trip counts.

### Screenshots
Place screenshots under `apps/streamlit/screenshots/` and reference them here:
- Overview: `apps/streamlit/screenshots/overview.png`
- Routes: `apps/streamlit/screenshots/routes.png`
- Weekdays: `apps/streamlit/screenshots/weekdays.png`
- Stations & Map: `apps/streamlit/screenshots/stations_map.png`
