"""
multi_api_pipeline.py

Beginner-friendly Airflow DAG demonstrating a full ETL cycle:
  1. Extract  -> pull data from two independent free public APIs
  2. Transform -> join both payloads into a single daily record
  3. Load     -> upsert the record into a Postgres warehouse table

APIs used (no API key required):
  - Open-Meteo         : current weather for a city
  - Frankfurter        : USD -> INR exchange rate

Run schedule: once per day (@daily). catchup=False so it only runs
going forward from when you unpause it.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

CITY = "Bengaluru"
LATITUDE = 12.9716
LONGITUDE = 77.5946

default_args = {
    "owner": "anagha",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="multi_api_daily_pipeline",
    description="Daily ETL: weather API + FX rate API -> Postgres warehouse",
    schedule="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "portfolio", "beginner"],
)
def multi_api_daily_pipeline():

    @task
    def fetch_weather() -> dict:
        """Extract: current weather reading from Open-Meteo."""
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": LATITUDE,
            "longitude": LONGITUDE,
            "current": "temperature_2m,relative_humidity_2m,wind_speed_10m",
            "timezone": "Asia/Kolkata",
        }
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        current = resp.json()["current"]
        logger.info("Weather fetched: %s", current)
        return {
            "temperature_c": current["temperature_2m"],
            "humidity_pct": current["relative_humidity_2m"],
            "wind_speed_kmh": current["wind_speed_10m"],
        }

    @task
    def fetch_exchange_rate() -> dict:
        """Extract: latest USD -> INR rate from Frankfurter."""
        url = "https://api.frankfurter.app/latest"
        params = {"from": "USD", "to": "INR"}
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        logger.info("Exchange rate fetched: %s", data)
        return {
            "usd_to_inr": data["rates"]["INR"],
            "rate_date": data["date"],
        }

    @task
    def transform_and_join(weather: dict, fx: dict) -> dict:
        """Transform: merge both sources into one row-shaped record."""
        record = {
            "run_date": datetime.utcnow().strftime("%Y-%m-%d"),
            "city": CITY,
            "temperature_c": weather["temperature_c"],
            "humidity_pct": weather["humidity_pct"],
            "wind_speed_kmh": weather["wind_speed_kmh"],
            "usd_to_inr": fx["usd_to_inr"],
            "fx_rate_date": fx["rate_date"],
        }
        logger.info("Joined record: %s", record)
        return record

    @task
    def load_to_postgres(record: dict) -> None:
        """Load: upsert into the warehouse. Requires a Postgres connection
        named 'warehouse_postgres' (configured via env var in docker-compose)."""
        hook = PostgresHook(postgres_conn_id="warehouse_postgres")
        insert_sql = """
            INSERT INTO daily_snapshot
                (run_date, city, temperature_c, humidity_pct, wind_speed_kmh, usd_to_inr, fx_rate_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_date) DO UPDATE SET
                temperature_c   = EXCLUDED.temperature_c,
                humidity_pct    = EXCLUDED.humidity_pct,
                wind_speed_kmh  = EXCLUDED.wind_speed_kmh,
                usd_to_inr      = EXCLUDED.usd_to_inr,
                fx_rate_date    = EXCLUDED.fx_rate_date;
        """
        hook.run(
            insert_sql,
            parameters=(
                record["run_date"],
                record["city"],
                record["temperature_c"],
                record["humidity_pct"],
                record["wind_speed_kmh"],
                record["usd_to_inr"],
                record["fx_rate_date"],
            ),
        )
        logger.info("Loaded record into warehouse: %s", record)

    weather_data = fetch_weather()
    fx_data = fetch_exchange_rate()
    joined_record = transform_and_join(weather_data, fx_data)
    load_to_postgres(joined_record)


multi_api_daily_pipeline()
