"""Tests for configuration loading."""

from __future__ import annotations

from pathlib import Path

from retail_lakehouse.config.settings import (
    load_data_generation_config,
    load_postgres_table_config,
)


def test_load_data_generation_config_from_yaml() -> None:
    config = load_data_generation_config(
        config_path=Path("config/data_generation.yaml")
    )
    assert config.num_customers > 0
    assert config.num_products > 0
    assert config.num_orders > 0
    assert config.order_start_date < config.order_end_date


def test_load_postgres_table_config() -> None:
    table_config = load_postgres_table_config(
        config_path=Path("config/postgres_tables.yaml")
    )
    assert table_config.schema == "retail"
    assert "customers" in table_config.tables
    assert table_config.batch_size > 0
