.PHONY: env up down logs ps psql reset

env:
	test -f .env || cp .env.example .env

up: env
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

ps:
	docker compose ps

psql:
	docker compose exec postgres sh -c 'psql -U $$POSTGRES_USER -d $$POSTGRES_DB'

# Destroys the Postgres volume (all warehouse and Airflow data).
reset:
	docker compose down -v
