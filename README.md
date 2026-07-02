# Multi-API Daily ETL Pipeline

A beginner data engineering project: an Airflow-orchestrated ETL pipeline
that pulls data from two independent public APIs, joins them, and loads
the result into a Postgres warehouse — visualized in a Streamlit dashboard.

## Architecture

```
Open-Meteo API  ---\
                     >--  Airflow DAG (extract -> transform -> load)  -->  Postgres warehouse  -->  Streamlit dashboard
Frankfurter API ---/
```

- **Orchestration:** Apache Airflow 2.9 (TaskFlow API, LocalExecutor)
- **Sources:** Open-Meteo (weather, no key needed), Frankfurter (FX rates, no key needed)
- **Storage:** Postgres, upserted daily by natural key (`run_date`)
- **Visualization:** Streamlit + Plotly

## Why this project

Demonstrates the core data engineering fundamentals: scheduled orchestration,
API extraction, transformation/joins across sources, idempotent loading
(upsert on rerun), retries, and a downstream consumption layer.

## Setup

### 1. Start Airflow + warehouse Postgres

```bash
docker compose up -d
```

Wait ~30-60 seconds for `airflow-init` to finish creating the metadata DB and
admin user, then check status with `docker compose ps`.

### 2. Open the Airflow UI

Go to `http://localhost:8080` and log in with `admin` / `admin`.

Find the DAG named `multi_api_daily_pipeline`, unpause it (toggle on the left),
and trigger a manual run (play button) to test it immediately rather than
waiting for the daily schedule.

### 3. Verify data landed in the warehouse

```bash
docker exec -it multi-api-pipeline-warehouse_db-1 psql -U warehouse -d warehouse -c "SELECT * FROM daily_snapshot;"
```

(Container name may differ slightly — check with `docker ps`.)

### 4. Run the dashboard

In a separate terminal, on your host machine (not inside Docker):

```bash
pip install -r requirements-dashboard.txt
streamlit run dashboard/app.py
```

Open the URL Streamlit prints (usually `http://localhost:8501`).

## Project structure

```
multi-api-pipeline/
├── dags/
│   └── multi_api_pipeline.py   # the Airflow DAG
├── sql/
│   └── init.sql                # warehouse schema, auto-run on first container start
├── dashboard/
│   └── app.py                  # Streamlit dashboard
├── docker-compose.yml
├── requirements-dashboard.txt
└── README.md
```

## Ideas to extend (good for a v2 / interview talking points)

- Add a third API source (e.g. a public holidays API) to show multi-source joins
- Add Great Expectations or dbt tests for data quality checks
- Add Slack/email alerting on task failure
- Swap Postgres for Snowflake/BigQuery to show cloud warehouse experience
- Add a backfill DAG run to demonstrate historical data loading
- Containerize the dashboard too, and add it as a fourth service in docker-compose

## Resume bullet (starter, edit with your own numbers once run for a while)

"Built and orchestrated a daily ETL pipeline in Apache Airflow ingesting data
from two external REST APIs, transforming and loading into a Postgres
warehouse with idempotent upserts, retries, and a Streamlit visualization
layer for monitoring pipeline output."
