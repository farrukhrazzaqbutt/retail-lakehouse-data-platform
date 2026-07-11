"""Daily end-to-end retail lakehouse pipeline DAG."""

from __future__ import annotations

import os
import sys
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

sys.path.insert(0, os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "include"))

from task_commands import pipeline_env, script_command  # noqa: E402

DEFAULT_ARGS = {
    "owner": "retail-data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="retail_daily_pipeline",
    description="Daily medallion pipeline: Postgres → Bronze → Silver → Gold → Snowflake → dbt",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["retail", "daily", "etl"],
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

    with TaskGroup(group_id="phase1_source") as phase1_source:
        load_postgres = BashOperator(
            task_id="load_postgres",
            bash_command=script_command("load_postgres.py", "--truncate"),
            env=pipeline_env(),
        )
        validate_phase1 = BashOperator(
            task_id="validate_phase1",
            bash_command=script_command("validate_phase1.py"),
            env=pipeline_env(),
        )
        load_postgres >> validate_phase1

    with TaskGroup(group_id="phase2_bronze") as phase2_bronze:
        generate_file_sources = BashOperator(
            task_id="generate_file_sources",
            bash_command=script_command("generate_file_sources.py"),
            env=pipeline_env(),
        )
        run_local_ingestion = BashOperator(
            task_id="run_local_ingestion",
            bash_command=script_command("run_local_ingestion.py"),
            env=pipeline_env(),
        )
        validate_phase2 = BashOperator(
            task_id="validate_phase2",
            bash_command=script_command("validate_phase2.py"),
            env=pipeline_env(),
        )
        generate_file_sources >> run_local_ingestion >> validate_phase2

    with TaskGroup(group_id="phase4_silver") as phase4_silver:
        run_silver_transforms = BashOperator(
            task_id="run_silver_transforms",
            bash_command=script_command("run_silver_transforms.py"),
            env=pipeline_env(),
        )
        validate_silver = BashOperator(
            task_id="validate_silver",
            bash_command=script_command("validate_silver.py"),
            env=pipeline_env(),
        )
        run_silver_transforms >> validate_silver

    with TaskGroup(group_id="phase5_gold") as phase5_gold:
        run_gold_models = BashOperator(
            task_id="run_gold_models",
            bash_command=script_command("run_gold_models.py"),
            env=pipeline_env(),
        )
        validate_gold = BashOperator(
            task_id="validate_gold",
            bash_command=script_command("validate_gold.py"),
            env=pipeline_env(),
        )
        run_gold_models >> validate_gold

    with TaskGroup(group_id="phase6_snowflake") as phase6_snowflake:
        run_snowflake_load = BashOperator(
            task_id="run_snowflake_load",
            bash_command=script_command("run_snowflake_load.py"),
            env=pipeline_env(),
        )
        validate_snowflake = BashOperator(
            task_id="validate_snowflake",
            bash_command=script_command("validate_snowflake.py"),
            env=pipeline_env(),
        )
        run_snowflake_load >> validate_snowflake

    with TaskGroup(group_id="phase7_dbt") as phase7_dbt:
        run_dbt_models = BashOperator(
            task_id="run_dbt_models",
            bash_command=script_command("run_dbt_models.py"),
            env=pipeline_env(),
        )
        validate_dbt = BashOperator(
            task_id="validate_dbt",
            bash_command=script_command("validate_dbt.py"),
            env=pipeline_env(),
        )
        run_dbt_models >> validate_dbt

    run_reconciliation = BashOperator(
        task_id="run_reconciliation",
        bash_command=script_command("run_reconciliation.py"),
        env=pipeline_env(),
    )

    end = EmptyOperator(task_id="end")

    (
        start
        >> wait_for_postgres
        >> phase1_source
        >> phase2_bronze
        >> phase4_silver
        >> phase5_gold
        >> phase6_snowflake
        >> phase7_dbt
        >> run_reconciliation
        >> end
    )
