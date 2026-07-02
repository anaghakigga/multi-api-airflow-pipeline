
Readme · MD
# Daily Weather & FX Rate Pipeline
 
An automated ETL pipeline that ingests live weather and currency exchange rate data from two public REST APIs every day, transforms and loads it into a Postgres warehouse, and surfaces it through an interactive dashboard — orchestrated end-to-end with Apache Airflow.
 
**Live stack:** Apache Airflow · PostgreSQL · Docker · Streamlit · Plotly · Python
 
---
 
## What it does
 
Every day, the pipeline:
1. **Extracts** current weather data (Open-Meteo) and the live USD→INR exchange rate (Frankfurter)
2. **Transforms** both responses into a single unified record
3. **Loads** it into a Postgres warehouse table via an idempotent upsert (safe to re-run without creating duplicates)
4. **Serves** the accumulated data through a Streamlit dashboard with live metrics and trend charts
## Architecture
 
```
Open-Meteo API  ─┐
                  ├──►  Airflow DAG  ──►  Postgres warehouse  ──►  Streamlit dashboard
Frankfurter API ─┘     (extract →
                        transform →
                        load)
```
 
## Key features
 
- **Scheduled orchestration** — Airflow DAG on a daily cron schedule, built with the modern TaskFlow API
- **Idempotent loads** — upsert logic on a natural key (`run_date`) means re-running a DAG never creates duplicate rows
- **Fault tolerance** — automatic retries with backoff on task failure
- **Decoupled architecture** — ingestion, storage, and visualization run as independent services via Docker Compose
- **Multi-source join** — combines two unrelated third-party APIs into one clean, queryable schema
## Tech stack
 
| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 2.9 (TaskFlow API, LocalExecutor) |
| Storage | PostgreSQL |
| Containerization | Docker & Docker Compose |
| Visualization | Streamlit + Plotly |
| Language | Python |