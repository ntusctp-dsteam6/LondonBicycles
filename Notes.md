Additional PIP Installs:

pip install --upgrade pandas google-cloud-bigquery db-dtypes

pip install pandas google-cloud-bigquery plotly

pip install dagster dagit

pip install db-dtypes

pip install --upgrade google-cloud-bigquery google-cloud-bigquery-storage db-dtypes

conda install -c conda-forge db-dtypes

pip install streamlit

pip install plotly

pip install google-cloud-bigquery-storage

pip install google-cloud-bigquery

pip install db-dtypes

source ge_env/bin/activate

streamlit run apps/streamlit/streamlit_londonbikes_app.py

export DAGSTER_HOME=/home/jayaprakashn/LondonBicycles/dagster_home
dagster-webserver -m orchestration.repository

dagster-daemon run ##in a separate shell