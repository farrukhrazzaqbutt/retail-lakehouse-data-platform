"""Tests for Airflow DAG import validation."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow")

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT / "airflow" / "include"))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from airflow.models import DagBag  # noqa: E402


def test_dag_bag_imports_without_errors(monkeypatch) -> None:
    """All DAG files should import cleanly in Airflow."""
    monkeypatch.setenv("AIRFLOW_HOME", str(PROJECT_ROOT / "airflow"))
    dag_folder = PROJECT_ROOT / "airflow" / "dags"
    bag = DagBag(
        dag_folder=str(dag_folder),
        include_examples=False,
    )
    assert not bag.import_errors, bag.import_errors
    expected = {
        "retail_platform_setup",
        "retail_daily_pipeline",
        "retail_health_check",
    }
    assert expected.issubset(set(bag.dags.keys()))
