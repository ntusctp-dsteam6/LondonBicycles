import os
import json
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from typing import Any, List


st.set_page_config(page_title="London Bikes Explorer", page_icon="🚲", layout="wide")


def sql_dt(field: str) -> str:
    """Return a robust DATE expression for mixed storage types of trip_start.

    Logic:
    - If the value can be interpreted as INT64 (millis), and within valid range,
      use DATE(TIMESTAMP_MILLIS(...)). If out of range, return NULL.
    - Otherwise, assume it's already a TIMESTAMP/DATE/DATETIME and use DATE(field).
      (Avoid unsupported CASTs like INT64 -> TIMESTAMP.)
    """
    f = field
    ms = f"SAFE_CAST({f} AS INT64)"
    within = f"({ms} BETWEEN 0 AND 253402300799000)"  # ~9999-12-31
    return (
        f"CASE WHEN {ms} IS NOT NULL THEN "
        f"  CASE WHEN {within} THEN DATE(TIMESTAMP_MILLIS({ms})) ELSE NULL END "
        f"ELSE DATE({f}) END"
    )


def make_daily_trips_sql(project: str, dataset: str, start_date: str, end_date: str,
                         start_station: str | None = None, end_station: str | None = None) -> str:
    ms = "SAFE_CAST(trip_start AS INT64)"
    dt = f"DATE(TIMESTAMP_MILLIS({ms}))"
    guards = [f"{ms} IS NOT NULL", f"{ms} BETWEEN 0 AND 253402300799000",
              f"{dt} BETWEEN DATE('{start_date}') AND DATE('{end_date}')"]
    if start_station:
        guards.append("start_station_name = '" + start_station.replace("'", "\\'") + "'")
    if end_station:
        guards.append("end_station_name = '" + end_station.replace("'", "\\'") + "'")
    where_sql = " AND ".join(guards)
    return f"""
SELECT {dt} AS dt, COUNT(*) AS trips
FROM `{project}.{dataset}.fact_trips`
WHERE {where_sql}
GROUP BY dt
ORDER BY dt
""".strip()


def make_top_stations_sql(project: str, dataset: str, n: int = 15, direction: str = "start") -> str:
    col = "start_station_name" if direction == "start" else "end_station_name"
    return f"""
SELECT {col} AS station, COUNT(*) AS trips
FROM `{project}.{dataset}.fact_trips`
GROUP BY {col}
ORDER BY trips DESC
LIMIT {n}
""".strip()


def make_duration_hist_sql(project: str, dataset: str) -> str:
    return f"""
SELECT
  CAST(ROUND(duration/60.0, 0) AS INT64) AS minutes_bin,
  COUNT(*) AS trips
FROM `{project}.{dataset}.fact_trips`
WHERE duration IS NOT NULL AND duration >= 0 AND duration <= 4*60*60
GROUP BY minutes_bin
ORDER BY minutes_bin
""".strip()


def make_dim_stations_sql(project: str, dataset: str) -> str:
    return f"""
SELECT station_id, station_name, latitude, longitude
FROM `{project}.{dataset}.dim_stations`
""".strip()


def make_top_routes_sql(project: str, dataset: str, limit: int = 20) -> str:
    return f"""
SELECT start_station_name AS start_station,
       end_station_name   AS end_station,
       COUNT(*)           AS trips
FROM `{project}.{dataset}.fact_trips`
GROUP BY start_station, end_station
ORDER BY trips DESC
LIMIT {limit}
""".strip()


def make_top_routes_map_sql(project: str, dataset: str, limit: int = 10) -> str:
    return f"""
WITH top_routes AS (
  SELECT start_station_id, end_station_id, COUNT(*) AS trips
  FROM `{project}.{dataset}.fact_trips`
  GROUP BY 1,2
  ORDER BY trips DESC
  LIMIT {limit}
)
SELECT
  r.trips,
  s1.station_name AS start_station,
  s1.latitude     AS start_lat,
  s1.longitude    AS start_lon,
  s2.station_name AS end_station,
  s2.latitude     AS end_lat,
  s2.longitude    AS end_lon
FROM top_routes r
JOIN `{project}.{dataset}.dim_stations` s1 ON s1.station_id = r.start_station_id
JOIN `{project}.{dataset}.dim_stations` s2 ON s2.station_id = r.end_station_id
""".strip()


def make_trips_by_weekday_sql(project: str, dataset: str) -> str:
    return f"""
WITH base AS (
  SELECT
    DATE(TIMESTAMP_MILLIS(SAFE_CAST(trip_start AS INT64))) AS dt
  FROM `{project}.{dataset}.fact_trips`
  WHERE SAFE_CAST(trip_start AS INT64) IS NOT NULL
    AND SAFE_CAST(trip_start AS INT64) BETWEEN 0 AND 253402300799000
)
SELECT FORMAT_DATE('%A', dt) AS day_of_week,
       COUNT(*)              AS trips
FROM base
GROUP BY day_of_week
""".strip()


def make_top_routes_by_weekday_sql(project: str, dataset: str, top_n: int = 5) -> str:
    return f"""
WITH base AS (
  SELECT
    DATE(TIMESTAMP_MILLIS(SAFE_CAST(trip_start AS INT64))) AS dt,
    start_station_name AS start_station,
    end_station_name   AS end_station
  FROM `{project}.{dataset}.fact_trips`
  WHERE SAFE_CAST(trip_start AS INT64) IS NOT NULL
    AND SAFE_CAST(trip_start AS INT64) BETWEEN 0 AND 253402300799000
), counts AS (
  SELECT FORMAT_DATE('%A', dt) AS day_of_week,
         start_station,
         end_station,
         COUNT(*) AS trips
  FROM base
  GROUP BY day_of_week, start_station, end_station
), ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY day_of_week ORDER BY trips DESC) AS rn
  FROM counts
)
SELECT day_of_week, start_station, end_station, trips
FROM ranked
WHERE rn <= {top_n}
ORDER BY 
  CASE day_of_week
    WHEN 'Monday' THEN 1 WHEN 'Tuesday' THEN 2 WHEN 'Wednesday' THEN 3
    WHEN 'Thursday' THEN 4 WHEN 'Friday' THEN 5 WHEN 'Saturday' THEN 6 WHEN 'Sunday' THEN 7
  END, trips DESC
""".strip()


def make_area_flows_sql(project: str, dataset: str, limit_pairs: int = 20) -> str:
    return f"""
WITH base AS (
  SELECT
    TRIM(SPLIT(s1.station_name, ',')[SAFE_OFFSET(1)]) AS start_area,
    TRIM(SPLIT(s2.station_name, ',')[SAFE_OFFSET(1)]) AS end_area,
    COUNT(*) AS trips
  FROM `{project}.{dataset}.fact_trips` f
  JOIN `{project}.{dataset}.dim_stations` s1 ON s1.station_id = f.start_station_id
  JOIN `{project}.{dataset}.dim_stations` s2 ON s2.station_id = f.end_station_id
  GROUP BY start_area, end_area
), ranked AS (
  SELECT *, ROW_NUMBER() OVER (ORDER BY trips DESC) AS rn
  FROM base
)
SELECT start_area, end_area, trips
FROM ranked
WHERE rn <= {limit_pairs}
""".strip()


def si_format(n: float) -> str:
    """Format large numbers with K/M suffix and one decimal place."""
    try:
        n = float(n)
    except Exception:
        return str(n)
    absn = abs(n)
    if absn >= 1_000_000:
        return f"{n/1_000_000:.1f}M"
    if absn >= 1_000:
        return f"{n/1_000:.1f}K"
    return f"{n:.0f}"


@st.cache_resource
def get_client(project_id: str):
    """Return a BigQuery client using one of several credential sources."""
    try:
        load_dotenv(override=False)
    except Exception:
        pass

    creds = None
    mode = "adc"
    try:
        if "gcp" in st.secrets:
            gcp = st.secrets["gcp"]
            if "credentials" in gcp and gcp["credentials"]:
                try:
                    info = gcp["credentials"]
                    info_dict = json.loads(info) if isinstance(info, str) else dict(info)
                    creds = service_account.Credentials.from_service_account_info(info_dict)
                    mode = "secrets-info"
                    if not project_id:
                        project_id = gcp.get("project_id", project_id)
                except Exception:
                    pass
            elif "key_path" in gcp and gcp["key_path"]:
                key_path = os.fspath(gcp["key_path"])
                if os.path.isfile(key_path):
                    creds = service_account.Credentials.from_service_account_file(key_path)
                    mode = "secrets-file"
                    if not project_id:
                        project_id = gcp.get("project_id", project_id)
    except Exception:
        pass

    if creds is None:
        key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if key_path and os.path.isfile(key_path):
            try:
                creds = service_account.Credentials.from_service_account_file(key_path)
                mode = "env-file"
            except Exception:
                creds = None

    if creds is not None:
        client = bigquery.Client(project=project_id, credentials=creds)
    else:
        client = bigquery.Client(project=project_id)
        mode = "adc"

    st.sidebar.caption(f"Auth: {mode} • Project: {project_id}")
    return client


@st.cache_data(ttl=600)
def _run_query_cached(project_id: str, sql: str) -> pd.DataFrame:
    client = get_client(project_id)
    return client.query(sql).to_dataframe(create_bqstorage_client=False)


def run_query(_client_or_project: Any, sql: str) -> pd.DataFrame:
    if isinstance(_client_or_project, str):
        project_id = _client_or_project
    else:
        project_id = getattr(_client_or_project, 'project', None) or \
                     os.getenv('DSAI_PROJECT_ID') or os.getenv('GOOGLE_CLOUD_PROJECT') or ''
    return _run_query_cached(project_id, sql)


def main():
    st.title("🚲 London Bikes Explorer")
    st.caption("Visualize trips, stations, and durations from BigQuery")

    default_project = os.getenv("DSAI_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT") or ""
    project = st.sidebar.text_input("GCP Project ID", value=default_project,
                                    help="Where your LondonBicycles dataset lives")
    dataset = st.sidebar.text_input("Dataset", value="LondonBicycles")

    if not project:
        st.warning("Set a valid Project ID (env DSAI_PROJECT_ID).")
        st.stop()

    # Default date range: 01/01/2021 to 31/12/2022, with dataset-bounds helper
    start_default = date(2021, 1, 1)
    end_default = date(2022, 12, 31)

    bounds_sql = f"""
    WITH base AS (
      SELECT DATE(TIMESTAMP_MILLIS(SAFE_CAST(trip_start AS INT64))) AS dt
      FROM `{project}.{dataset}.fact_trips`
      WHERE SAFE_CAST(trip_start AS INT64) IS NOT NULL
        AND SAFE_CAST(trip_start AS INT64) BETWEEN 0 AND 253402300799000
    )
    SELECT MIN(dt) AS min_dt, MAX(dt) AS max_dt FROM base
    """.strip()
    try:
      bdf = run_query(project, bounds_sql)
      min_dt = pd.to_datetime(bdf.loc[0, 'min_dt']).date() if not bdf.empty else start_default
      max_dt = pd.to_datetime(bdf.loc[0, 'max_dt']).date() if not bdf.empty else end_default
    except Exception:
      min_dt, max_dt = start_default, end_default

    dr = st.sidebar.date_input("Date range", value=(start_default, end_default), key="date_range")
    if isinstance(dr, tuple) and len(dr) == 2:
        start_d, end_d = dr
    else:
        start_d, end_d = start_default, dr if dr else end_default

    direction = st.sidebar.radio("Top stations by", options=["start", "end"], horizontal=True)
    n_top = st.sidebar.slider("Top N stations", min_value=5, max_value=30, value=15)

    tabs = st.tabs(["Overview", "Routes", "Weekdays", "Stations & Map"])

    daily_sql = make_daily_trips_sql(project, dataset, start_d.isoformat(), end_d.isoformat())
    df_daily = run_query(project, daily_sql)
    if df_daily.empty:
        st.sidebar.warning("No data in selected range. Use dataset bounds?")
        if st.sidebar.button(f"Set to dataset range ({min_dt} → {max_dt})"):
            st.session_state.date_range = (min_dt, max_dt)
            st.rerun()
    dur_sql = make_duration_hist_sql(project, dataset)
    df_dur = run_query(project, dur_sql)
    top_sql = make_top_stations_sql(project, dataset, n_top, direction)
    df_top = run_query(project, top_sql)

    total_trips = int(df_daily["trips"].sum()) if not df_daily.empty else 0
    avg_min = (round((df_dur["minutes_bin"] * df_dur["trips"]).sum() / df_dur["trips"].sum(), 1)
               if not df_dur.empty and df_dur["trips"].sum() > 0 else 0)
    top_peak = int(df_top["trips"].max() or 0) if not df_top.empty else 0

    with tabs[0]:
        k1, k2, k3 = st.columns(3)
        k1.metric("Total trips", si_format(total_trips))
        k2.metric("Avg duration (min)", avg_min)
        k3.metric(f"Top {direction} station trips", si_format(top_peak))

        st.markdown("---")
        c1, c2 = st.columns([2, 1])
        with c1:
            st.subheader("Trips over time")
            fig = px.line(df_daily, x="dt", y="trips", markers=True)
            fig.update_layout(height=400, margin=dict(l=20, r=20, b=40, t=40))
            y_max = max([0] + df_daily["trips"].tolist()) if not df_daily.empty else 0
            fig.update_yaxes(tickformat=".2s", range=[0, y_max * 1.1 if y_max else 1])
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            st.subheader(f"Top {n_top} {direction} stations")
            fig2 = px.bar(df_top, x="trips", y="station", orientation="h")
            fig2.update_layout(height=400, margin=dict(l=20, r=20, b=40, t=40))
            fig2.update_xaxes(tickformat=".2s")
            st.plotly_chart(fig2, use_container_width=True)

        st.subheader("Duration distribution (minutes)")
        fig3 = px.bar(df_dur, x="minutes_bin", y="trips")
        fig3.update_layout(height=350, margin=dict(l=20, r=20, b=40, t=20))
        fig3.update_yaxes(tickformat=".2s")
        st.plotly_chart(fig3, use_container_width=True)

    with tabs[1]:
        st.subheader("Most frequent routes")
        limit_routes = st.slider("Top routes", 5, 50, 20, step=5)
        routes_sql = make_top_routes_sql(project, dataset, limit_routes)
        df_routes = run_query(project, routes_sql)
        if not df_routes.empty:
            df_routes["route"] = df_routes["start_station"] + " → " + df_routes["end_station"]
            figr = px.bar(df_routes, x="trips", y="route", orientation="h")
            figr.update_layout(height=600, margin=dict(l=20, r=20, b=40, t=20))
            x_max = max([0] + df_routes["trips"].tolist())
            figr.update_xaxes(tickformat=".2s", range=[0, x_max * 1.1 if x_max else 1])
            st.plotly_chart(figr, use_container_width=True)
            st.dataframe(df_routes, use_container_width=True)
        else:
            st.info("No route data to display.")

        st.markdown("---")
        st.subheader("Top routes on the map")
        map_limit = st.slider("Routes on map", 5, 25, 10)
        rmap_sql = make_top_routes_map_sql(project, dataset, map_limit)
        df_rmap = run_query(project, rmap_sql)
        if not df_rmap.empty:
            import plotly.graph_objects as go
            figm = go.Figure()
            for _, row in df_rmap.iterrows():
                figm.add_trace(go.Scattermapbox(
                    lat=[row["start_lat"], row["end_lat"]],
                    lon=[row["start_lon"], row["end_lon"]],
                    mode="lines+markers",
                    line=dict(width=2),
                    marker=dict(size=6),
                    name=f"{row['start_station']} → {row['end_station']} ({si_format(row['trips'])})",
                    hoverinfo="name",
                ))
            figm.update_layout(mapbox_style="open-street-map", mapbox_zoom=10,
                               mapbox_center={"lat": float(df_rmap["start_lat"].mean()), "lon": float(df_rmap["start_lon"].mean())},
                               height=500, margin=dict(l=10, r=10, b=10, t=10))
            st.plotly_chart(figm, use_container_width=True)
        else:
            st.info("No route map data available.")

        st.markdown("---")
        st.subheader("Top area-to-area flows")
        pair_limit = st.slider("Top area pairs", 10, 100, 30, step=10)
        df_area = run_query(project, make_area_flows_sql(project, dataset, pair_limit))
        if not df_area.empty:
            top_areas: List[str] = sorted(set(df_area["start_area"]).union(df_area["end_area"]))[:15]
            piv = df_area[df_area["start_area"].isin(top_areas) & df_area["end_area"].isin(top_areas)] \
                .pivot_table(index="start_area", columns="end_area", values="trips", aggfunc="sum", fill_value=0)
            figh = px.imshow(piv, color_continuous_scale="viridis", aspect="auto")
            figh.update_layout(height=500, margin=dict(l=20, r=20, b=40, t=40))
            st.plotly_chart(figh, use_container_width=True)
            with st.expander("Show table"):
                st.dataframe(df_area, use_container_width=True)
        else:
            st.info("No area flow data available.")

    with tabs[2]:
        st.subheader("Trips by weekday")
        wsql = make_trips_by_weekday_sql(project, dataset)
        df_wd = run_query(project, wsql)
        if not df_wd.empty:
            order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            df_wd["day_of_week"] = pd.Categorical(df_wd["day_of_week"], categories=order, ordered=True)
            df_wd = df_wd.sort_values("day_of_week")
            figw = px.bar(df_wd, x="day_of_week", y="trips")
            y_maxw = max([0] + df_wd["trips"].tolist())
            figw.update_yaxes(tickformat=".2s", range=[0, y_maxw * 1.1 if y_maxw else 1])
            st.plotly_chart(figw, use_container_width=True)
        else:
            st.info("No weekday data.")

        st.markdown("---")
        st.subheader("Top routes per weekday")
        df_wr = run_query(project, make_top_routes_by_weekday_sql(project, dataset, top_n=5))
        if not df_wr.empty:
            day = st.selectbox("Select day", ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"], index=0)
            sdf = df_wr[df_wr["day_of_week"] == day]
            sdf["route"] = sdf["start_station"] + " → " + sdf["end_station"]
            figwr = px.bar(sdf, x="trips", y="route", orientation="h")
            x_max2 = max([0] + sdf["trips"].tolist()) if not sdf.empty else 0
            figwr.update_xaxes(tickformat=".2s", range=[0, x_max2 * 1.1 if x_max2 else 1])
            st.plotly_chart(figwr, use_container_width=True)
            with st.expander("Show table"):
                st.dataframe(sdf, use_container_width=True)
        else:
            st.info("No per-weekday route data.")

    with tabs[3]:
        st.subheader("Stations map and net flows")
        st.caption("Bubble size ~ trips (start station)")
        stn_sql = make_dim_stations_sql(project, dataset)
        df_stn = run_query(project, stn_sql)
        if not df_stn.empty and not df_top.empty:
            df_top_start = run_query(project, make_top_stations_sql(project, dataset, n=50, direction="start"))
            merged = df_top_start.merge(df_stn, left_on="station", right_on="station_name", how="left")
            merged = merged.dropna(subset=["latitude", "longitude"])
            fig4 = px.scatter_mapbox(
                merged,
                lat="latitude",
                lon="longitude",
                size="trips",
                color="trips",
                hover_name="station",
                zoom=10,
                height=450,
                color_continuous_scale="viridis",
                size_max=35,
            )
            fig4.update_layout(mapbox_style="open-street-map", margin=dict(l=10, r=10, b=10, t=10))
            st.plotly_chart(fig4, use_container_width=True)
        else:
            st.info("No station data available yet.")


if __name__ == "__main__":
    main()

