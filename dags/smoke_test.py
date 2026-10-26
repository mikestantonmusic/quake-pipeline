"""Smoke test: confirms Airflow can reach the warehouse and the layer schemas exist."""

from datetime import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

EXPECTED_SCHEMAS = {"raw", "staging", "marts"}


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False, tags=["infra"])
def smoke_test():
    @task
    def check_warehouse() -> list[str]:
        hook = PostgresHook(postgres_conn_id="warehouse")
        rows = hook.get_records("select schema_name from information_schema.schemata")
        found = {r[0] for r in rows}
        missing = EXPECTED_SCHEMAS - found
        if missing:
            raise RuntimeError(f"Missing warehouse schemas: {sorted(missing)}")
        return sorted(EXPECTED_SCHEMAS)

    @task
    def check_dbt() -> None:
        import subprocess

        subprocess.run(["dbt", "--version"], check=True)

    check_warehouse()
    check_dbt()


smoke_test()
