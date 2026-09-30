FROM astrocrpublic.azurecr.io/runtime:3.2-6

# dbt + ingest tools in their own venv: no dependency fights with Airflow.
# dbt pinned to the version you built and tested with locally (Days 2–5).
RUN python -m venv /usr/local/airflow/tools_venv && \
    /usr/local/airflow/tools_venv/bin/pip install --no-cache-dir \
    "dbt-core==1.12.3" "dbt-databricks==1.12.5" databricks-sql-connector databricks-sdk pandas pyarrow requests
ENV DBT_PROFILES_DIR=/usr/local/airflow/include/dbt
# dbt writes target/ and logs/ inside the container, never into your mounted repo folder
ENV DBT_TARGET_PATH=/tmp/dbt_target DBT_LOG_PATH=/tmp/dbt_logs
