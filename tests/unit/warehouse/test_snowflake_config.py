"""Tests for Snowflake load configuration."""

from __future__ import annotations

from pathlib import Path

from retail_lakehouse.config.settings import (
    load_snowflake_config,
    load_snowflake_load_config,
)


def test_load_snowflake_load_config() -> None:
    config = load_snowflake_load_config(config_path=Path("config/snowflake_load.yaml"))
    assert config.gold_base_path == "gold"
    assert config.load_mode == "overwrite"
    assert len(config.load_order) == 3
    assert config.load_order[0].layer == "dimensions"
    assert "dim_customers" in config.load_order[0].tables
    assert "fct_orders" in config.load_order[1].tables
    assert "mart_daily_sales" in config.load_order[2].tables


def test_load_snowflake_config_defaults() -> None:
    config = load_snowflake_config()
    assert config.database == "RETAIL_DW"
    assert config.schema == "RAW"
