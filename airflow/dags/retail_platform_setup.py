"""One-time platform setup DAG for Snowflake and dbt schemas."""

from __future__ import annotations

import os
import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "include"))

from task_commands import pipeline_env, script_command  # noqa: E402

DEFAULT_ARGS = {
    "owner": "retail-data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="retail_platform_setup",
    description="One-time Snowflake and dbt schema provisioning",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["retail", "setup", "phase6", "phase7"],
    default_args=DEFAULT_ARGS,
) as dag:
    start = EmptyOperator(task_id="start")

    wait_for_postgres = BashOperator(
        task_id="wait_for_postgres",
        bash_command=(
            "until pg_isready -h ${POSTGRES_HOST} -p ${POSTGRES_PORT} "
            "-U ${POSTGRES_USER}; do sleep 2; done"
        ),
        env=pipeline_env(),
    )

    setup_snowflake = BashOperator(
        task_id="setup_snowflake",
        bash_command=script_command("setup_snowflake.py"),
        env=pipeline_env(),
    )

    setup_dbt_schemas = BashOperator(
        task_id="setup_dbt_schemas",
        bash_command=script_command("setup_dbt_schemas.py"),
        env=pipeline_env(),
    )

    end = EmptyOperator(task_id="end")

    start >> wait_for_postgres >> setup_snowflake >> setup_dbt_schemas >> end
