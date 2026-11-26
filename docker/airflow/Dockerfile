FROM apache/airflow:3.0.6-python3.12

# Pipeline dependencies, installed against Airflow's constraints to avoid version conflicts.
COPY docker/airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.6/constraints-3.12.txt" \
    -r /requirements.txt

# dbt lives in its own virtualenv so its dependencies never clash with Airflow's.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.12.6 dbt-snowflake==1.12.5
ENV PATH="${PATH}:/home/airflow/dbt-venv/bin"

# dbt packages (dbt_utils), installed once here instead of at the start of every dbt task.
# dbt_project.yml reads DBT_PACKAGES_DIR, because ./dbt is mounted over this copy at runtime.
# Rebuild the image after changing packages.yml.
ENV DBT_PACKAGES_DIR=/home/airflow/dbt_packages
# --chown: the image runs as the airflow user, and dbt writes logs next to the project files.
COPY --chown=airflow:root dbt/quake/dbt_project.yml dbt/quake/packages.yml dbt/quake/package-lock.yml /tmp/dbt-deps/
RUN cd /tmp/dbt-deps && /home/airflow/dbt-venv/bin/dbt deps --no-use-colors \
    && rm -rf /tmp/dbt-deps
