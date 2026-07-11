"""Tests for Airflow DAG structure."""

from __future__ import annotations

import ast
from pathlib import Path

AIRFLOW_DAGS = Path("airflow/dags")
EXPECTED_DAGS = {
    "retail_platform_setup",
    "retail_daily_pipeline",
    "retail_health_check",
}


def _dag_id_from_file(path: Path) -> str | None:
    """Extract dag_id from a DAG file using AST parsing."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.With):
            continue
        for item in node.items:
            if not isinstance(item.context_expr, ast.Call):
                continue
            call = item.context_expr
            if not (isinstance(call.func, ast.Name) and call.func.id == "DAG"):
                continue
            for keyword in call.keywords:
                if keyword.arg == "dag_id" and isinstance(keyword.value, ast.Constant):
                    return str(keyword.value.value)
    return None


def test_expected_dag_files_exist() -> None:
    dag_files = list(AIRFLOW_DAGS.glob("*.py"))
    assert len(dag_files) == 3
    dag_ids = {_dag_id_from_file(path) for path in dag_files}
    assert dag_ids == EXPECTED_DAGS


def test_daily_pipeline_has_phase_task_groups() -> None:
    source = (AIRFLOW_DAGS / "retail_daily_pipeline.py").read_text(encoding="utf-8")
    for group in (
        "phase1_source",
        "phase2_bronze",
        "phase4_silver",
        "phase5_gold",
        "phase6_snowflake",
        "phase7_dbt",
    ):
        assert group in source


def test_task_commands_module(monkeypatch) -> None:
    import sys

    for key in (
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    ):
        monkeypatch.delenv(key, raising=False)

    sys.path.insert(0, str(Path("airflow/include")))
    from task_commands import pipeline_env, script_command

    env = pipeline_env()
    assert env["POSTGRES_HOST"] == "postgres"
    command = script_command("validate_phase1.py")
    assert "scripts/validate_phase1.py" in command
