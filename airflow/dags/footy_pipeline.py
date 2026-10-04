"""Daily: pull current-season CSVs, check source freshness, build dbt project."""
from __future__ import annotations

import logging
import subprocess
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sdk import DAG, TaskGroup, Variable

DIVS = ["E0", "E1", "SP1", "D1", "I1", "F1"]
TOOLS = "/usr/local/airflow/tools_venv/bin"   # dbt + ingest venv, built in the Dockerfile
DBT_DIR = "/usr/local/airflow/dbt/footy_wh"
INGEST = "/usr/local/airflow/ingest/load_raw.py"

log = logging.getLogger(__name__)


def on_failure(context):
    ti = context["task_instance"]
    log.error("FAILED task=%s dag=%s run=%s try=%s",
              ti.task_id, ti.dag_id, context["run_id"], ti.try_number)
    # production: post to Slack / Teams from here


def load_div(div: str, **context) -> None:
    # blank param = the normal daily run; a season typed into the trigger form = a backfill.
    # the Variable is read at RUN time, never at parse time.
    season = context["params"].get("season") or Variable.get("footy_current_season", default="2627")
    subprocess.run([f"{TOOLS}/python", INGEST, "--season", season, "--divs", div], check=True)


default_args = {
    "owner": "gil",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "on_failure_callback": on_failure,
}

with DAG(
    dag_id="footy_pipeline",
    description="football-data.co.uk → UC volume → Delta raw → dbt marts",
    start_date=datetime(2026, 9, 1),
    schedule="0 6 * * *",          # 06:00 UTC daily; matches are finished, site is updated
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    params={"season": ""},
    tags=["footy", "dbt"],
) as dag:

    with TaskGroup("ingest") as ingest:
        for d in DIVS:
            PythonOperator(task_id=f"load_{d}", python_callable=load_div, op_kwargs={"div": d},
                           pool="raw_matches_writer")   # COPY INTO commits conflict on one table

    dbt_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"cd {DBT_DIR} && {TOOLS}/dbt source freshness --target prod",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && {TOOLS}/dbt build --target prod",
        execution_timeout=timedelta(minutes=30),   # a hung build fails loudly instead of blocking tomorrow
    )

    site_up = HttpSensor(
        task_id="site_available",
        http_conn_id="football_data",
        endpoint="mmz4281/{{ params.season or var.value.footy_current_season }}/E0.csv",
        method="HEAD",
        mode="reschedule",      # frees the worker slot between checks; "poke" would hold it the whole wait
        poke_interval=300,      # check every 5 minutes
        timeout=3600,           # give up after an hour
    )

    site_up >> ingest >> dbt_freshness >> dbt_build
