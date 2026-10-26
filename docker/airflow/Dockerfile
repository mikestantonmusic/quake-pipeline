FROM apache/airflow:3.0.6-python3.12

# Pipeline dependencies, installed against Airflow's constraints to avoid version conflicts.
COPY docker/airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.6/constraints-3.12.txt" \
    -r /requirements.txt

# dbt lives in its own virtualenv so its dependencies never clash with Airflow's.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-postgres==1.9.0
ENV PATH="${PATH}:/home/airflow/dbt-venv/bin"
