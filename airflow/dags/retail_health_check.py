"""Weekly health-check DAG for dry-run and compile-only validation."""

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
    "retries": 0,
}

with DAG(
    dag_id="retail_health_check",
    description="Weekly pipeline health checks without Snowflake writes",
    schedule="0 6 * * 1",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["retail", "health", "monitoring"],
    default_args=DEFAULT_ARGS,
) as dag:
    start = EmptyOperator(task_id="start")

    snowflake_dry_run = BashOperator(
        task_id="snowflake_load_dry_run",
        bash_command=script_command("run_snowflake_load.py", "--dry-run"),
        env=pipeline_env(),
    )

    dbt_compile = BashOperator(
        task_id="dbt_compile_only",
        bash_command=script_command("validate_dbt.py", "--compile-only"),
        env=pipeline_env(),
    )

    reconciliation_report = BashOperator(
        task_id="reconciliation_report",
        bash_command=script_command(
            "run_reconciliation.py", "--skip-snowflake"
        ),
        env=pipeline_env(),
    )

    end = EmptyOperator(task_id="end")

    start >> [snowflake_dry_run, dbt_compile, reconciliation_report] >> end
