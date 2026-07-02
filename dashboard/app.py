"""
Simple Streamlit dashboard for the multi-API pipeline.
Run locally with: streamlit run dashboard/app.py
(Requires the warehouse_db container to be running and exposed on localhost:5433)
"""

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

st.set_page_config(page_title="Daily Weather + FX Pipeline", layout="wide")


@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame:
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="warehouse",
        user="warehouse",
        password="warehouse",
    )
    df = pd.read_sql("SELECT * FROM daily_snapshot ORDER BY run_date;", conn)
    conn.close()
    return df


st.title("Daily Weather + FX Rate Pipeline Dashboard")
st.caption("Ingested daily via Airflow from Open-Meteo and Frankfurter APIs")

df = load_data()

if df.empty:
    st.warning("No data yet. Unpause the DAG in the Airflow UI and let it run once.")
else:
    col1, col2, col3 = st.columns(3)
    col1.metric("Latest Temp (°C)", df.iloc[-1]["temperature_c"])
    col2.metric("Latest USD → INR", df.iloc[-1]["usd_to_inr"])
    col3.metric("Days of Data", len(df))

    st.plotly_chart(
        px.line(df, x="run_date", y="temperature_c", title="Temperature over time"),
        use_container_width=True,
    )
    st.plotly_chart(
        px.line(df, x="run_date", y="usd_to_inr", title="USD to INR over time"),
        use_container_width=True,
    )
    st.dataframe(df)
