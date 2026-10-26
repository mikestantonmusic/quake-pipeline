# USGS Earthquake Pipeline

An end-to-end batch data pipeline built on the [USGS earthquake feeds](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php).

**Stack:** Docker Compose · Python · Airflow 3 · Postgres 16 · dbt · Streamlit

```
USGS feed / FDSN API
   │  Airflow: extract
   ▼
data/raw/YYYY/MM/DD/*.geojson     raw files (immutable)
   │  Airflow: load
   ▼
Postgres raw.*                    append-only JSONB
   │  Airflow: dbt build
   ▼
staging.* → marts.*               deduplicated, typed, modeled
   │
   ▼
Streamlit dashboard
```

## Quick start

Requires [Docker Desktop](https://www.docker.com/products/docker-desktop/).

```sh
cp .env.example .env          # PowerShell: Copy-Item .env.example .env
docker compose up -d --build  # or: make up
```

| Service   | URL                    |
|-----------|------------------------|
| Airflow   | http://localhost:8080  |
| Streamlit | http://localhost:8501  |
| Postgres  | localhost:5432 (credentials in `.env`) |

In Airflow, unpause and trigger the `smoke_test` DAG. It should pass when the warehouse schemas exist and dbt is installed.

## Layout

| Path | Purpose |
|------|---------|
| `ingestion/` | Pure-Python extract and load code (no Airflow imports, unit-testable) |
| `dags/` | Airflow DAGs |
| `dbt/` | dbt project (staging and marts) |
| `dashboard/` | Streamlit app |
| `docker/` | Airflow image and Postgres init scripts |
| `data/` | Raw GeoJSON landing zone (git-ignored) |
