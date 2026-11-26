"""Smoke test: confirms Airflow can reach S3 and Snowflake, and that dbt is installed.

Uses only metadata calls, so it doesn't start the Snowflake warehouse.
"""

from datetime import datetime

from airflow.sdk import dag, task

EXPECTED_SCHEMAS = {"RAW", "STAGING", "MARTS"}


@dag(schedule=None, start_date=datetime(2026, 1, 1), catchup=False, tags=["infra"])
def smoke_test():
    @task
    def check_s3() -> str:
        import os

        import boto3

        bucket = os.environ["RAW_BUCKET"]
        boto3.client("s3").list_objects_v2(Bucket=bucket, Prefix="raw/", MaxKeys=1)
        return bucket

    @task
    def check_snowflake() -> list[str]:
        from ingestion.snowflake_load import connect_from_env

        conn = connect_from_env()
        try:
            rows = conn.cursor().execute("show schemas in database").fetchall()
        finally:
            conn.close()
        found = {r[1] for r in rows}
        missing = EXPECTED_SCHEMAS - found
        if missing:
            raise RuntimeError(f"Missing Snowflake schemas: {sorted(missing)}")
        return sorted(EXPECTED_SCHEMAS)

    @task
    def check_dbt() -> None:
        import subprocess

        subprocess.run(["dbt", "--version"], check=True)

    check_s3()
    check_snowflake()
    check_dbt()


smoke_test()
