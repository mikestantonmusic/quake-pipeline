.PHONY: env build up down logs ps psql reset

env:
	test -f .env || cp .env.example .env

# Builds the Airflow image with plain `docker build`: `docker compose up --build` fails on
# recent Docker Desktop versions (bake needs an extra file-read permission).
build:
	docker build -f docker/airflow/Dockerfile -t quake-pipeline/airflow:local .
	docker build -t quake-pipeline-streamlit ./dashboard

up: env build
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

ps:
	docker compose ps

psql:
	docker compose exec postgres sh -c 'psql -U $$POSTGRES_USER -d $$POSTGRES_DB'

# Destroys the Postgres volume (Airflow's run history). Snowflake and S3 are untouched.
reset:
	docker compose down -v
