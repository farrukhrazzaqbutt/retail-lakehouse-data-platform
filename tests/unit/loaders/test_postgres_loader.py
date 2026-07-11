"""Tests for PostgreSQL loader."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from retail_lakehouse.config.settings import PostgresConfig, load_postgres_table_config
from retail_lakehouse.loaders.postgres_loader import PostgresLoader, TABLE_LOAD_ORDER


@pytest.fixture
def loader() -> PostgresLoader:
    """Create a loader with test configuration."""
    postgres_config = PostgresConfig(
        host="localhost",
        port=55432,
        database="retail_db",
        user="retail_user",
        password="secret",
        schema="retail",
    )
    table_config = load_postgres_table_config(
        config_path=Path("config/postgres_tables.yaml")
    )
    return PostgresLoader(postgres_config, table_config)


def test_table_load_order_respects_foreign_keys() -> None:
    assert TABLE_LOAD_ORDER.index("customers") < TABLE_LOAD_ORDER.index("orders")
    assert TABLE_LOAD_ORDER.index("orders") < TABLE_LOAD_ORDER.index("order_items")


def test_load_table_empty_returns_zero(loader: PostgresLoader) -> None:
    rows = loader.load_table("customers", pd.DataFrame())
    assert rows == 0


def test_engine_requires_password() -> None:
    postgres_config = PostgresConfig(
        host="localhost",
        port=55432,
        database="retail_db",
        user="retail_user",
        password="",
        schema="retail",
    )
    table_config = load_postgres_table_config(
        config_path=Path("config/postgres_tables.yaml")
    )
    loader = PostgresLoader(postgres_config, table_config)
    with pytest.raises(ValueError, match="POSTGRES_PASSWORD"):
        _ = loader.engine
