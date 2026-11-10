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

## Local development (no Docker needed)

The ingestion code is plain Python, so it can be run and tested directly:

```sh
python -m venv .venv
.venv\Scripts\activate            # macOS/Linux: source .venv/bin/activate
pip install -r requirements-dev.txt

pytest                            # unit tests (offline; HTTP is mocked)
ruff check .                      # lint

# Download data into data/raw/
python -m ingestion.extract feed --level all --period day
python -m ingestion.extract range --start 2026-10-01 --end 2026-10-02
```

Raw files are saved unchanged and named after the window they cover, so re-running an extract overwrites rather than duplicates:

```
data/raw/feed/all_day/YYYY/MM/DD/<run time>.geojson
data/raw/fdsn/YYYY/MM/DD/<window start>_<window end>.geojson
```

The FDSN API returns at most 20,000 events per query, so `range` checks the count first and halves the window until each part fits.

### Loading into Postgres

With the Docker stack running:

```sh
python -m ingestion.load                         # load everything under data/raw
python -m ingestion.load data/raw/fdsn/2026/10   # or specific files/folders
```

`raw.earthquake_events` stores one row per event **version**, keyed on `(event_id, updated_ms)`. USGS bumps `updated` whenever it revises an event (new magnitude, status `automatic` → `reviewed`), so revisions become new rows. Seeing the same version again, which happens constantly because the hourly `all_day` feed overlaps itself, inserts nothing. That makes loads idempotent: re-running a load never creates duplicates. Each load is recorded in `raw.load_audit`.

Database tests run against a separate `warehouse_test` database and are skipped automatically when Postgres isn't reachable.

## Transforming with dbt

dbt runs inside the Airflow container, which has it installed and can reach Postgres:

```sh
docker compose exec -w /opt/airflow/dbt/quake airflow-scheduler dbt deps    # first time only
docker compose exec -w /opt/airflow/dbt/quake airflow-scheduler dbt build   # build and test everything
```

| Table | Grain | Purpose |
|-------|-------|---------|
| `staging.stg_earthquakes` | one row per event | Latest version of every event, flattened from JSON into typed columns |
| `marts.fct_earthquakes` | one row per earthquake | Main analysis table; excludes blasts and deleted events; adds `mag_class`, `depth_class`, `region` |
| `marts.dim_region` | one row per region | Lifetime stats per region |
| `marts.agg_daily_by_region` | one row per region per day | Daily counts and magnitudes |

Design decisions:

- **Incremental staging, re-ranked per event.** Each run reprocesses every stored version of any event with new rows, not only the new rows, so an older version loaded late by a backfill can't overwrite a newer one.
- **Deletions propagate.** A version with status `deleted` removes the event from the marts while staying visible in staging.
- **Reconciliation tests.** Besides column tests, `tests/` asserts that `fct_earthquakes` matches the latest versions in raw, and that the daily aggregate adds up to the fact table.
- **`region` comes from `place`.** It's the text after the last comma, or the whole string when there's no comma ("Banda Sea"). The `region_aliases` seed expands codes like `CA` and `MX`. It's a label parsed from USGS text, not a true geographic join.

## Orchestrating with Airflow

Two DAGs run the full pipeline. Both are paused when first created; unpause them in the Airflow UI (http://localhost:8080).

| DAG | Schedule | What it does |
|-----|----------|--------------|
| `quake_incremental` | hourly | Downloads the `all_day` feed, loads new event versions, rebuilds the marts |
| `quake_backfill` | manual | Downloads a date range from the FDSN API, loads it, rebuilds the marts |

```
extract → load → transform (dbt, one task per model via Cosmos)
                   region_aliases.seed → test ─┐
                   stg_earthquakes.run → test ─┴→ fct_earthquakes.run → test ─┬→ dim_region.run → test
                                                                              └→ agg_daily_by_region.run → test
```

To backfill, trigger `quake_backfill` with a config such as `{"start": "2026-10-01", "end": "2026-10-08"}` (UTC dates, end exclusive):

```sh
docker compose exec airflow-scheduler airflow dags trigger quake_backfill --conf '{"start": "2026-10-01", "end": "2026-10-08"}'
```

Design decisions:

- **Thin DAGs.** Tasks call the same `ingestion` functions as the CLI. Only file paths and small summaries pass between tasks (XCom), never the data itself.
- **Per-model dbt tasks with [Cosmos](https://github.com/astronomer/astronomer-cosmos).** Each model and its tests are separate Airflow tasks, so a failure shows exactly which model broke and can be retried on its own. Tests use `BUILDABLE` selection, so a test that reads several models only runs once all of them are rebuilt.
- **A one-slot `dbt` pool.** All dbt tasks share it, so an hourly run and a backfill never build the same incremental table at the same time.
- **Safe retries.** Every task is idempotent: extract files are named after the run, and loads skip versions that are already stored.

## Layout

| Path | Purpose |
|------|---------|
| `ingestion/` | Pure-Python extract and load code (no Airflow imports, unit-testable) |
| `dags/` | Airflow DAGs |
| `dbt/` | dbt project (staging and marts) |
| `dashboard/` | Streamlit app |
| `docker/` | Airflow image and Postgres init scripts |
| `data/` | Raw GeoJSON landing zone (git-ignored) |
