-- Warehouse table that the Airflow DAG loads into.
-- run_date is the natural key: one row per day, upserted on rerun.

CREATE TABLE IF NOT EXISTS daily_snapshot (
    run_date        DATE PRIMARY KEY,
    city            TEXT NOT NULL,
    temperature_c   NUMERIC,
    humidity_pct    NUMERIC,
    wind_speed_kmh  NUMERIC,
    usd_to_inr      NUMERIC,
    fx_rate_date    DATE
);
