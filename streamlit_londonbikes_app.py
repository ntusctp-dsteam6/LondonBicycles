import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
from google.cloud import bigquery_storage
from dotenv import load_dotenv
import os
from datetime import date
import plotly.graph_objects as go
import numpy as np
from PIL import Image



# -----------------------------
# Page Config
# -----------------------------
st.set_page_config(page_title="London Bikes Analytics", layout="wide")
st.title("🚴 London Bikes Analytics")


# Load image
img_path = "/Users/anjan/DSAI_Project_Group6/LondonBicycles/Image.png"
img = Image.open(img_path)

# Crop height by half (keep top half)
width, height = img.size
img_cropped = img.crop((0, 0, width, height // 2))  # (left, top, right, bottom)

# Display in Streamlit
st.image(img_cropped, use_container_width=True)

# -----------------------------
# Setup
# -----------------------------
load_dotenv()
project_id = os.environ.get("DSAI_PROJECT_ID")
analytics_dataset = "LondonBicycles_Analytics"

client = bigquery.Client(project=project_id)
bqstorage_client = bigquery_storage.BigQueryReadClient()

@st.cache_data(show_spinner=True)
def load_table(table_name, last_12_months_only=False):
    query = f"SELECT * FROM `{project_id}.{analytics_dataset}.{table_name}`"
    return client.query(query).to_dataframe()

# -----------------------------
# Load Tables
# -----------------------------
daily_df = load_table("daily_summaries")
hourly_df = load_table("hourly_counts")
top_stations_df = load_table("top_stations")
duration_df = load_table("trip_duration_histogram")
route_df = load_table("route_popularity")
duration_band_df = load_table("duration_band")
return_origin_df = load_table("return_to_origin")
supply_demand_df = load_table("station_demand_supply_gap")
station_static = load_table("station_static")

# Combine station trips across all months/years
top_stations_agg = top_stations_df.groupby('station_name')[['trips_started','trips_ended']].sum().reset_index()
top_stations_agg['total_trips'] = top_stations_agg['trips_started'] + top_stations_agg['trips_ended']

# Determine latest year for supply/demand analysis
latest_year = supply_demand_df['year'].max()
supply_latest_year = supply_demand_df[supply_demand_df['year']==latest_year -1]

# -----------------------------
# Tabs
# -----------------------------
tabs = st.tabs(["Overall Trends", "Stations & Routes", "Trip Duration & Return", "Supply & Net Inflow"])

# -----------------------------
# Tab 1: Overall Trends
# -----------------------------
with tabs[0]:
    st.header("📊 Overall Trend Analysis")
    col1, col2, col3 = st.columns(3)

    # Compute average trips per year
    avg_trips_per_year = daily_df.groupby('year')['trip_count'].sum().mean()
    col1.metric("Average Trips per Year", f"{avg_trips_per_year:,.0f}")

    col2.metric("Avg Duration (min)", f"{daily_df['avg_duration_minutes'].mean():.2f}")


    # Compute year with maximum total trips
    year_max_trips = daily_df.groupby('year')['trip_count'].sum().idxmax()
    total_trips_max_year = daily_df.groupby('year')['trip_count'].sum().max()
    col3.metric("Year with Max Trips", f"{year_max_trips}", f"{total_trips_max_year:,} trips")


    # Prepare monthly data
    monthly_df = (
        daily_df.groupby(['year', 'month'], as_index=False)['trip_count']
        .sum()
    )

    # Keep only the last 5 years
    last_5_years = sorted(monthly_df['year'].unique())[-5:]
    monthly_df = monthly_df[monthly_df['year'].isin(last_5_years)]

    # Compute monthly average per year
    yearly_avg_df = (
        monthly_df.groupby('year', as_index=False)['trip_count']
        .mean()
        .rename(columns={'trip_count': 'monthly_avg'})
    )
    monthly_df = monthly_df.merge(yearly_avg_df, on='year', how='left')

    # Create a combined period column for x-axis
    monthly_df['period'] = pd.to_datetime(
        monthly_df['year'].astype(str) + '-' + monthly_df['month'].astype(str) + '-01'
    )

    # Convert year to string so Plotly uses distinct colors
    monthly_df['year'] = monthly_df['year'].astype(str)

    # --- Plot ---
    fig_monthly_bar = px.bar(
        monthly_df,
        x='period',
        y='trip_count',
        color='year',
        title="🌈 Monthly Trips (Last 5 Years) with Yearly Average Overlay",
        color_discrete_sequence=px.colors.qualitative.Bold,  # Vibrant colors
        labels={'total_trips': 'Trips', 'period': 'Month', 'year': 'Year'}
    )

    # Make the bars thinner (~15 days width)
    fig_monthly_bar.update_traces(width=15 * 24 * 60 * 60 * 1000, selector=dict(type='bar'))

    # Add yearly average overlay line
    fig_monthly_bar.add_scatter(
        x=monthly_df['period'],
        y=monthly_df['monthly_avg'],
        mode='lines+markers',
        name='Yearly Avg Trips',
        line=dict(color='black', width=3, dash='dot'),
        marker=dict(size=6, color='black', symbol='circle')
    )

    # Improve layout
    fig_monthly_bar.update_layout(
        xaxis_title="Month",
        yaxis_title="Total Trips",
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        legend_title="Year",
        bargap=0.25
    )

    st.plotly_chart(fig_monthly_bar, use_container_width=True)



    # Derive quarter from the hourly_df
    hourly_df['quarter'] = pd.to_datetime(hourly_df['date']).dt.quarter

    # Compute average trip_count by hour and quarter
    hourly_qtr_agg = (
        hourly_df
        .groupby(['quarter', 'trip_hour'], as_index=False)['trip_count']
        .mean()
    )

    # Ensure all combinations of quarters and hours exist
    all_combos = pd.MultiIndex.from_product([range(1, 5), range(24)], names=['quarter', 'trip_hour'])
    hourly_qtr_agg = hourly_qtr_agg.set_index(['quarter', 'trip_hour']).reindex(all_combos).reset_index()
    hourly_qtr_agg['trip_count'] = hourly_qtr_agg['trip_count'].fillna(0)

    # Plot with color by quarter
    fig_hourly_qtr = px.line(
        hourly_qtr_agg,
        x='trip_hour',
        y='trip_count',
        color='quarter',       # 🔹 One line per quarter
        title="Average Hourly Trips by Quarter",
        markers=True
    )
    fig_hourly_qtr.update_xaxes(dtick=1)  # show all hours on x-axis

    st.plotly_chart(fig_hourly_qtr, use_container_width=True)

# -----------------------------
# Tab 2: Stations & Routes
# -----------------------------
with tabs[1]:
    st.header("🏙️ Station Traffic Ranking")
    # --- 2x2 Grid Stats ---
    col1, col2 = st.columns(2)
    col3, col4 = st.columns(2)

    # 1. Total Stations
    col1.metric("🏙️ Total Stations", f"{top_stations_df['station_name'].nunique():,}")

    # 2. Total Areas
    col2.metric("📍 Total Areas", f"{top_stations_df['station_area'].nunique():,}")

    # 3. Top Area
    busiest_area = (
        top_stations_df.groupby('station_area')[['trips_started', 'trips_ended']]
        .sum()
    )
    busiest_area['total_trips'] = busiest_area['trips_started'] + busiest_area['trips_ended']
    top_area = busiest_area['total_trips'].idxmax()
    col3.metric("🌟 Top Area", top_area)

    # 4. % of Trips in Top Area
    top_area_pct = busiest_area['total_trips'].max() / busiest_area['total_trips'].sum() * 100
    col4.metric("📊 % Trips in Top Area", f"{top_area_pct:.1f}%")

    # --- Compute total traffic (inflow + outflow) ---
    top_stations_agg['total_traffic'] = top_stations_agg['trips_started'] + top_stations_agg['trips_ended']

    # --- Compute daily average traffic ---
    if 'date' in daily_df.columns:
        num_days = (daily_df['date'].max() - daily_df['date'].min()).days + 1
    else:
        num_days = len(daily_df)

    top_stations_agg['avg_daily_traffic'] = top_stations_agg['total_traffic'] / num_days

    # --- Slider for top N stations ---
    top_n = st.slider("Select Top N Stations by Avg Daily Traffic", 5, 25, 10)

    # --- Pick top stations by avg daily traffic ---
    top_traffic_stations = top_stations_agg.nlargest(top_n, 'avg_daily_traffic')

    fig_top_avg_traffic = px.bar(
        top_traffic_stations,
        x='station_name',
        y='avg_daily_traffic',
        color='avg_daily_traffic',
        text='avg_daily_traffic',           # add values on top of bars
        title=f"Top {top_n} Stations by Average Daily Traffic (Inflows + Outflows)",
        labels={'avg_daily_traffic': 'Avg Daily Traffic'},
    )

    fig_top_avg_traffic.update_traces(texttemplate='%{text:.0f}', textposition='outside')
    fig_top_avg_traffic.update_layout(yaxis_title="Average Daily Trips")

    st.plotly_chart(fig_top_avg_traffic, use_container_width=True)

    st.header("🗺️ Top Station Areas Map with Individual Stations & Tourist Spots")

    # Slider for top N areas
    top_area_n = st.slider("Top N Areas to Highlight", 1, 10, 5)

    # Compute total trips per station
    top_stations_df['total_trips'] = top_stations_df['trips_started'] + top_stations_df['trips_ended']

    # Aggregate total trips per area and pick top N areas
    area_agg = (
        top_stations_df.groupby('station_area')['total_trips'].sum().reset_index()
        .sort_values('total_trips', ascending=False)
        .head(top_area_n)
    )

    # Assign letters based on ranking
    area_agg['area_letter'] = [chr(65+i) for i in range(len(area_agg))]
    area_letter_map = dict(zip(area_agg['station_area'], area_agg['area_letter']))

    # Filter stations belonging to top N areas
    stations_in_top_areas = top_stations_df[top_stations_df['station_area'].isin(area_agg['station_area'])]

    # Keep only top 5 stations per area
    stations_in_top_areas = stations_in_top_areas.sort_values(['station_area', 'total_trips'], ascending=[True, False])
    stations_in_top_areas = stations_in_top_areas.groupby('station_area').head(5)

    # Map color per area
    area_colors = px.colors.qualitative.Set1
    area_unique = stations_in_top_areas['station_area'].unique()
    area_color_map = {area: area_colors[i % len(area_colors)] for i, area in enumerate(area_unique)}

    # Add area letters to stations
    stations_in_top_areas['area_letter'] = stations_in_top_areas['station_area'].map(area_letter_map)

    # Top 8 tourist destinations in London
    tourist_df = pd.DataFrame({
        'name': [
            'London Eye', 'British Museum', 'Tower of London', 'Buckingham Palace',
            'Big Ben', 'Trafalgar Square', "St Paul's Cathedral", 'Natural History Museum'
        ],
        'latitude': [51.5033, 51.5194, 51.5081, 51.5014, 51.5007, 51.5080, 51.5138, 51.4967],
        'longitude': [-0.1195, -0.1270, -0.0759, -0.1419, -0.1246, -0.1281, -0.0984, -0.1764]
    })
    tourist_df['number'] = range(1, len(tourist_df)+1)

    # Create figure
    fig_map = go.Figure()

    # Add stations: one trace per area
    for area in area_agg['station_area']:
        area_stations = stations_in_top_areas[stations_in_top_areas['station_area'] == area]
        fig_map.add_trace(go.Scattermapbox(
            lat=area_stations['latitude'],
            lon=area_stations['longitude'],
            mode='markers+text',
            marker=dict(
                size=area_stations['total_trips'] / area_stations['total_trips'].max() * 40 + 5,
                color=area_color_map[area]
            ),
            text=area_stations['area_letter'],       # show A/B/C inside bubble
            textposition='middle center',
            name=f"Area {area_letter_map[area]}: {area}",
            hovertext=area_stations['station_name'],  # station name in hover
            hoverinfo='text'
        ))

    # Add tourist spots with black bubbles, numbered
    fig_map.add_trace(go.Scattermapbox(
        lat=tourist_df['latitude'],
        lon=tourist_df['longitude'],
        mode='markers+text',
        marker=dict(size=14, color='black'),
        text=tourist_df['number'].astype(str),
        textposition='middle center',
        name='Tourist Spot',
        hovertext=tourist_df['name'],
        hoverinfo='text'
    ))

    # Map layout
    fig_map.update_layout(
        mapbox_style="open-street-map",
        mapbox_zoom=12,
        mapbox_center={"lat": stations_in_top_areas['latitude'].mean(),
                    "lon": stations_in_top_areas['longitude'].mean()},
        height=650,
        margin={"r":0,"t":30,"l":0,"b":0},
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01)
    )
    st.plotly_chart(fig_map, use_container_width=True)

    # Tourist spot legend below map
    tourist_legend_text = "<br>".join([f"{num}. {name}" for num, name in zip(tourist_df['number'], tourist_df['name'])])
    st.markdown(f"**Tourist Spots Legend:**<br>{tourist_legend_text}", unsafe_allow_html=True)


    # -----------------------------
    # Heatmap: Top Start & End Routes by Hour
    # -----------------------------
    st.header("🔥 Top Start-End Routes Heatmap by Hour")

    # Hour filter dropdown
    selected_hour_1 = st.selectbox(
        "Select Hour to Analyze",
        options=list(range(24)),
        index=8,  # default 8 AM
        key="heatmap_hour",  # unique key for this selectbox
        format_func=lambda x: f"{x}:00 - {x}:59"
    )

    # Filter route_df by selected hour
    route_hour_df = route_df[route_df['trip_hour'] == selected_hour_1]

    # Compute total trips per start and end station
    route_agg = route_hour_df.groupby(['start_station_name', 'end_station_name'])['trip_count'].sum().reset_index()

    # Pick top 10 start stations by total trips
    top_start_stations = (
        route_agg.groupby('start_station_name')['trip_count'].sum()
        .nlargest(10)
        .index
    )

    # Pick top 10 end stations by total trips
    top_end_stations = (
        route_agg.groupby('end_station_name')['trip_count'].sum()
        .nlargest(10)
        .index
    )

    # Filter the data
    route_heatmap_df = route_agg[
        route_agg['start_station_name'].isin(top_start_stations) &
        route_agg['end_station_name'].isin(top_end_stations)
    ]

    # Create pivot table for heatmap
    heatmap_data = route_heatmap_df.pivot(index='start_station_name', columns='end_station_name', values='trip_count').fillna(0)

    # Plot heatmap
    fig_heatmap = px.imshow(
        heatmap_data,
        text_auto=True,
        aspect="auto",
        color_continuous_scale='Viridis',
        labels=dict(x="End Station", y="Start Station", color="Trips"),
        title=f"Top Start-End Station Trip Counts (Hour {selected_hour_1}:00)"
    )

    st.plotly_chart(fig_heatmap, use_container_width=True)

# -----------------------------
# Tab 3: Trip Duration & Return
# -----------------------------
with tabs[2]:

    st.header("⏱️ Trip Duration & Return to Origin Analysis")

    # Filter duration <= 60 min
    duration_df_filtered = hourly_df.copy()
    duration_df_filtered['duration_min'] = duration_df_filtered['avg_duration_minutes']
    duration_df_filtered = duration_df_filtered[(duration_df_filtered['duration_min'] > 0) &
                                                (duration_df_filtered['duration_min'] <= 60)]

    # --- Key Metrics ---
    pct_below_30 = (duration_df_filtered['duration_min'] <= 30).mean() * 100
    yearly_avg_duration = duration_df_filtered.groupby('year')['duration_min'].mean().reset_index()
    top_year_row = yearly_avg_duration.loc[yearly_avg_duration['duration_min'].idxmax()]

    col1, col2, col3 = st.columns(3)
    col1.metric("% Trips ≤30 min", f"{pct_below_30:.1f}%")
    col2.metric("Year with Top Avg Duration", f"{int(top_year_row['year'])}")
    col3.metric("Top Yearly Avg Duration (min)", f"{top_year_row['duration_min']:.1f}")

    # -----------------------------
    # Duration Bands Bar Chart with Correct %
    # -----------------------------
    # Define duration bands
    bins = [0, 5, 15, 30, 45, 60, float('inf')]
    labels = ['Under 5 min', '5-15 min', '15-30 min', '30-45 min', '45-60 min', 'Over 60 min']

    # Bin the durations
    duration_df_filtered['duration_band'] = pd.cut(
        duration_df_filtered['duration_min'],
        bins=bins,
        labels=labels,
        right=True
    )

    # Compute counts per band
    duration_band_df = (
        duration_df_filtered.groupby('duration_band')
        .size()
        .reset_index(name='trip_count')
    )

    # Compute % for each band
    duration_band_df['pct'] = (duration_band_df['trip_count'] / duration_band_df['trip_count'].sum() * 100).round(1)

    # Create bar chart
    fig_duration_band = px.bar(
        duration_band_df,
        x='duration_band',
        y='trip_count',
        color='duration_band',
        color_discrete_sequence=px.colors.sequential.Plasma_r,
        title='Trip Duration Distribution by Band',
        category_orders={'duration_band': labels},
        text='pct'
    )

    # Add % labels on top
    fig_duration_band.update_traces(
        texttemplate='%{text}%', 
        textposition='outside'
    )

    # Increase top margin
    fig_duration_band.update_layout(
        yaxis_title='Trip Count',
        margin=dict(t=80, b=40, l=60, r=40)
    )

    st.plotly_chart(fig_duration_band, use_container_width=True)

    # --- Violin Plot with improved colors ---
    fig_violin = px.violin(
        duration_df_filtered,
        y='duration_min',
        box=True,
        points='all',
        color_discrete_sequence=['#636EFA'],  # Blue color
        title="Trip Duration Distribution (Violin Plot, <= 60 min)",
        labels={'duration_min': 'Trip Duration (minutes)'}
    )
    fig_violin.update_traces(meanline_visible=True)
    st.plotly_chart(fig_violin, use_container_width=True)

    # --- Hour-of-Day vs Avg Duration ---
    hourly_avg = duration_df_filtered.groupby('trip_hour')['duration_min'].mean().reset_index()
    fig_hourly = px.line(
        hourly_avg,
        x='trip_hour',
        y='duration_min',
        markers=True,
        title='Average Trip Duration by Hour of Day',
        labels={'trip_hour': 'Hour of Day', 'duration_min': 'Avg Trip Duration (min)'},
        color_discrete_sequence=['#EF553B']
    )
    st.plotly_chart(fig_hourly, use_container_width=True)



# =============================
# TAB 4: Hourly Inflow/Outflow Utilization (Last 12 Months)
# =============================
with tabs[3]:
    st.header("⏱️ Hourly Inflow/Outflow Utilization (Last 12 Months)")

    # -----------------------------
    # Filter last 12 months
    # -----------------------------
    route_df['year_month'] = route_df['year']*100 + route_df['month']
    last_12_ym = sorted(route_df['year_month'].unique())[-12:]
    route_12m_df = route_df[route_df['year_month'].isin(last_12_ym)]

    # -----------------------------
    # Select Hour Filter
    # -----------------------------
    selected_hour = st.selectbox(
        "Select Hour to Analyze",
        options=list(range(24)),
        index=8,  # default 8 AM
        format_func=lambda x: f"{x}:00 - {x}:59"
    )

    # -----------------------------
    # Compute inflow per station per day/hour
    # -----------------------------
    inflow_df = (
        route_12m_df.groupby(['end_station_name', 'year', 'month', 'day', 'trip_hour'], as_index=False)
        ['trip_count'].sum()
        .rename(columns={'end_station_name': 'station_name', 'trip_count': 'inflow'})
    )

    # -----------------------------
    # Compute outflow per station per day/hour
    # -----------------------------
    outflow_df = (
        route_12m_df.groupby(['start_station_name', 'year', 'month', 'day', 'trip_hour'], as_index=False)
        ['trip_count'].sum()
        .rename(columns={'start_station_name': 'station_name', 'trip_count': 'outflow'})
    )

    # -----------------------------
    # Strip station names
    # -----------------------------
    inflow_df['station_name'] = inflow_df['station_name'].str.strip()
    outflow_df['station_name'] = outflow_df['station_name'].str.strip()
    station_static['station_name'] = station_static['station_name'].str.strip()

    # -----------------------------
    # Merge inflow + outflow per station/day/hour
    # -----------------------------
    hourly_net_df = pd.merge(
        inflow_df,
        outflow_df,
        on=['station_name', 'year', 'month', 'day', 'trip_hour'],
        how='outer'
    ).fillna(0)

    # Filter by selected hour
    hourly_net_df = hourly_net_df[hourly_net_df['trip_hour'] == selected_hour]

    # Compute net flow per station/hour/day
    hourly_net_df['net_flow'] = hourly_net_df['inflow'] - hourly_net_df['outflow']

    # -----------------------------
    # Aggregate over all days → average for selected hour
    # -----------------------------
    station_metrics = hourly_net_df.groupby('station_name').agg(
        avg_hourly_inflow=('inflow', 'mean'),
        avg_hourly_outflow=('outflow', 'mean'),
        avg_hourly_net=('net_flow', 'mean')
    ).reset_index()

    # Merge docks_count from station_static
    station_metrics = station_metrics.merge(
        station_static[['station_name', 'docks_count']],
        on='station_name', how='left'
    )

    # Exclude stations with 0 docks
    station_metrics = station_metrics[station_metrics['docks_count'] > 0]

    # -----------------------------
    # Compute utilization per dock
    # -----------------------------
    station_metrics['utilization_net'] = station_metrics['avg_hourly_net'] / station_metrics['docks_count']
    station_metrics['utilization_inflow'] = station_metrics['avg_hourly_inflow'] / station_metrics['docks_count']
    station_metrics['utilization_outflow'] = station_metrics['avg_hourly_outflow'] / station_metrics['docks_count']
    station_metrics['utilization_total'] = (
        station_metrics['avg_hourly_inflow'] + station_metrics['avg_hourly_outflow']
    ) / station_metrics['docks_count']

    # -----------------------------
    # Slider: top N stations
    # -----------------------------
    top_n = st.slider("Top N Stations", 5, 20, 10)

    # -----------------------------
    # Graph 1: Top Net Utilization
    # -----------------------------
    top_imbalance = station_metrics.reindex(
        station_metrics['utilization_net'].abs().sort_values(ascending=False).index
    ).head(top_n)

    # Add station label with docks
    top_imbalance['station_label'] = top_imbalance.apply(
        lambda x: f"{x['station_name']} ({x['docks_count']} docks)",
        axis=1
    )

    fig_imbalance = px.bar(
        top_imbalance,
        x='station_label',
        y='utilization_net',
        color='utilization_net',
        color_continuous_scale=px.colors.sequential.RdBu,
        title=f"Top {top_n} Stations by Net Utilization (Hour {selected_hour}:00)",
        labels={'utilization_net': 'Net Utilization (per dock)'},
    )
    st.plotly_chart(fig_imbalance, use_container_width=True)

    # Add business explanation
    st.markdown("""
    **Interpretation: Net Utilization per Dock**  
    - Positive values → More bikes coming in than leaving → station fills up quickly, may run out of space.  
    - Negative values → More bikes leaving than coming in → station empties quickly, may run out of bikes.  
    - High absolute values → extreme imbalance; consider prioritizing these stations for **rebalancing** or **dock adjustments**.
    """)

    # -----------------------------
    # Graph 2: Top Total Traffic Utilization
    # -----------------------------
    top_traffic = station_metrics.nlargest(top_n, 'utilization_total')

    # Add station label with docks
    top_traffic['station_label'] = top_traffic.apply(
        lambda x: f"{x['station_name']} ({x['docks_count']} docks)",
        axis=1
    )

    fig_traffic = px.bar(
        top_traffic,
        x='station_label',
        y='utilization_total',
        color='utilization_total',
        color_continuous_scale=px.colors.sequential.Viridis,
        title=f"Top {top_n} Stations by Total Traffic Utilization (Hour {selected_hour}:00)",
        labels={'utilization_total': 'Total Utilization (per dock)'},
    )
    st.plotly_chart(fig_traffic, use_container_width=True)

    # Add business explanation
    st.markdown("""
    **Interpretation: Total Traffic Utilization per Dock**  
    - Measures the intensity of activity relative to dock capacity, ignoring direction.  
    - High values → very busy stations; may require **maintenance, staffing, or dock expansion**.  
    - Compare with net utilization to determine if a station is both busy and imbalanced during peak hours.
    """)