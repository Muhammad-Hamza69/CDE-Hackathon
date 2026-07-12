"""Task 2.4: Streamlit dashboard over the Snowflake Gold (and Silver, for
point-level detail) layers. Three required charts:
  1. Device activity map      - CLEAN.IOT_VALIDATED (latest position/device)
  2. Event-activity trend     - CLEAN.IOT_VALIDATED (substitutes for "AQI
                                 trend": this geoLocation dataset has no
                                 air-quality field, so the analogous
                                 time-series metric is event volume per
                                 device over time - documented in README)
  3. Top-N devices             - ANALYTICS.DEVICE_DAILY (daily aggregates)

Auto-refreshes every 30s (Task 2.4) via streamlit-autorefresh, which forces
a full script rerun so every query below re-executes against live data.
"""
import os

import pandas as pd
import plotly.express as px
import pydeck as pdk
import snowflake.connector
import streamlit as st
from streamlit_autorefresh import st_autorefresh

REFRESH_MS = 30_000

# Fixed categorical order (dataviz skill: assign hues in fixed order, never
# cycled/re-derived from filtered results) - first 5 slots cover the 5
# simulated devices from Task 1.2.
DEVICE_PALETTE = ["#2a78d6", "#1baf7a", "#eda100", "#008300", "#4a3aa7", "#e34948", "#e87ba4", "#eb6834"]
SEQUENTIAL_BLUE = "#2a78d6"

st.set_page_config(page_title="IoT Fleet Dashboard", layout="wide")
st_autorefresh(interval=REFRESH_MS, key="auto_refresh")


@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database="HACKATHON_IOT",
    )


@st.cache_data(ttl=25)
def query_df(sql: str) -> pd.DataFrame:
    conn = get_connection()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        cols = [c[0].lower() for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        cur.close()


def device_color_map(device_ids: list[str]) -> dict[str, str]:
    ordered = sorted(device_ids)
    return {d: DEVICE_PALETTE[i % len(DEVICE_PALETTE)] for i, d in enumerate(ordered)}


st.title("IoT Fleet Dashboard")
st.caption("Snowflake Gold layer - Task 2.4 - auto-refreshes every 30s")

latest_positions = query_df(
    """
    select device_id, latitude, longitude, event_ts
    from clean.iot_validated
    qualify row_number() over (partition by device_id order by event_ts desc) = 1
    """
)

trend = query_df(
    """
    select device_id, date_trunc('hour', event_ts) as event_hour, count(*) as event_count
    from clean.iot_validated
    where event_ts >= dateadd('day', -1, current_timestamp())
    group by device_id, event_hour
    order by event_hour
    """
)

daily = query_df(
    """
    select device_id, event_date, event_count
    from analytics.device_daily
    order by event_date desc
    """
)

colors = device_color_map(sorted(set(latest_positions["device_id"]).union(trend["device_id"])))

col1, col2 = st.columns([3, 2])

with col1:
    st.subheader("Device activity map")
    if latest_positions.empty:
        st.info("No validated events yet - waiting on the pipeline.")
    else:
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=latest_positions.assign(
                color=latest_positions["device_id"].map(lambda d: [int(colors[d][i : i + 2], 16) for i in (1, 3, 5)])
            ),
            get_position="[longitude, latitude]",
            get_fill_color="color",
            get_radius=25,
            pickable=True,
        )
        view_state = pdk.ViewState(
            latitude=float(latest_positions["latitude"].mean()),
            longitude=float(latest_positions["longitude"].mean()),
            zoom=15,
        )
        st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view_state, tooltip={"text": "{device_id}"}))

with col2:
    st.subheader("Latest reading per device")
    st.dataframe(latest_positions, hide_index=True, use_container_width=True)

st.subheader("Event-activity trend (last 24h)")
st.caption('Substitutes for "AQI trend" - this geoLocation dataset has no air-quality field.')
if trend.empty:
    st.info("No events in the last 24h yet.")
else:
    fig_trend = px.line(
        trend,
        x="event_hour",
        y="event_count",
        color="device_id",
        color_discrete_map=colors,
        markers=True,
    )
    fig_trend.update_traces(line_width=2)
    fig_trend.update_layout(
        legend_title_text="Device",
        xaxis_title="Hour",
        yaxis_title="Events",
        margin=dict(l=10, r=10, t=10, b=10),
    )
    st.plotly_chart(fig_trend, use_container_width=True)

st.subheader("Top-N devices (by daily event count)")
if daily.empty:
    st.info("No gold-layer aggregates yet - run `dbt run` after CDC data lands.")
else:
    top_n = (
        daily.groupby("device_id", as_index=False)["event_count"]
        .sum()
        .sort_values("event_count", ascending=False)
        .head(10)
    )
    fig_top = px.bar(top_n, x="device_id", y="event_count", color_discrete_sequence=[SEQUENTIAL_BLUE])
    fig_top.update_layout(
        xaxis_title="Device",
        yaxis_title="Total events",
        margin=dict(l=10, r=10, t=10, b=10),
    )
    st.plotly_chart(fig_top, use_container_width=True)
