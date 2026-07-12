"""Tests for dbt project structure and configuration."""

from __future__ import annotations

from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DBT_DIR = PROJECT_ROOT / "dbt"
EXPECTED_STAGING = [
    "stg_raw__dim_date.sql",
    "stg_raw__dim_customers.sql",
    "stg_raw__dim_products.sql",
    "stg_raw__dim_country.sql",
    "stg_raw__fct_orders.sql",
    "stg_raw__fct_order_items.sql",
    "stg_raw__fct_payments.sql",
]
EXPECTED_INTERMEDIATE = [
    "int_orders_enriched.sql",
    "int_order_items_enriched.sql",
    "int_orders_daily.sql",
    "int_payments_daily.sql",
    "int_customer_first_order.sql",
    "int_customer_segment_orders.sql",
]
EXPECTED_MARTS = [
    "mart_daily_sales.sql",
    "mart_monthly_revenue.sql",
    "mart_customer_lifetime_value.sql",
    "mart_product_performance.sql",
    "mart_customer_segments.sql",
]


def test_dbt_project_file_exists() -> None:
    project_file = DBT_DIR / "dbt_project.yml"
    assert project_file.exists()
    raw = yaml.safe_load(project_file.read_text(encoding="utf-8"))
    assert raw["name"] == "retail_lakehouse"
    assert "staging" in raw["models"]["retail_lakehouse"]
    assert "marts" in raw["models"]["retail_lakehouse"]


def test_dbt_profiles_use_env_vars() -> None:
    profiles = yaml.safe_load(
        (DBT_DIR / "profiles" / "profiles.yml").read_text(encoding="utf-8")
    )
    dev = profiles["retail_lakehouse"]["outputs"]["dev"]
    assert "env_var('SNOWFLAKE_ACCOUNT')" in dev["account"]
    assert dev["type"] == "snowflake"


def test_expected_model_files_exist() -> None:
    staging_dir = DBT_DIR / "models" / "staging" / "raw"
    intermediate_dir = DBT_DIR / "models" / "intermediate"
    marts_dir = DBT_DIR / "models" / "marts"

    for filename in EXPECTED_STAGING:
        assert (staging_dir / filename).exists(), filename
    for filename in EXPECTED_INTERMEDIATE:
        assert (intermediate_dir / filename).exists(), filename
    for filename in EXPECTED_MARTS:
        assert (marts_dir / filename).exists(), filename


def test_dbt_packages_include_utils() -> None:
    packages = yaml.safe_load((DBT_DIR / "packages.yml").read_text(encoding="utf-8"))
    package_names = [item["package"] for item in packages["packages"]]
    assert "dbt-labs/dbt_utils" in package_names


def test_dbt_parse_if_installed() -> None:
    import os
    import shutil
    import subprocess

    import pytest

    if shutil.which("dbt") is None:
        pytest.skip("dbt not installed")

    probe = subprocess.run(
        ["dbt", "--version"],
        capture_output=True,
        text=True,
        check=False,
    )
    if probe.returncode != 0:
        pytest.skip(f"dbt CLI unavailable: {probe.stderr}")

    env = {
        **os.environ,
        "DBT_PROFILES_DIR": str(DBT_DIR / "profiles"),  # absolute via PROJECT_ROOT
        "SNOWFLAKE_ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT", "xy12345.us-east-1"),
        "SNOWFLAKE_USER": os.getenv("SNOWFLAKE_USER", "placeholder"),
        "SNOWFLAKE_PASSWORD": os.getenv("SNOWFLAKE_PASSWORD", "placeholder"),
        "SNOWFLAKE_ROLE": os.getenv("SNOWFLAKE_ROLE", "placeholder"),
        "SNOWFLAKE_WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE", "placeholder"),
    }
    result = subprocess.run(
        ["dbt", "parse"],
        cwd=DBT_DIR,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
