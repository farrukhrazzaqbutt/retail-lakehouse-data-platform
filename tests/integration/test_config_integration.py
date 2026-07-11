"""Integration-style tests for configuration loading."""

from __future__ import annotations

from pathlib import Path

import pytest

from retail_lakehouse.config.settings import (
    load_adf_ingestion_config,
    load_data_generation_config,
    load_gold_model_config,
    load_postgres_table_config,
    load_silver_transform_config,
    load_snowflake_config,
    load_snowflake_load_config,
)


@pytest.mark.integration
@pytest.mark.parametrize(
    "loader,config_path",
    [
        (load_data_generation_config, "config/data_generation.yaml"),
        (load_postgres_table_config, "config/postgres_tables.yaml"),
        (load_adf_ingestion_config, "config/adf_ingestion.yaml"),
        (load_silver_transform_config, "config/silver_transforms.yaml"),
        (load_gold_model_config, "config/gold_models.yaml"),
        (load_snowflake_load_config, "config/snowflake_load.yaml"),
    ],
)
def test_yaml_configs_load(loader, config_path: str) -> None:
    """All phase YAML configs should load without error."""
    config = loader(config_path=Path(config_path))
    assert config is not None


def test_snowflake_config_defaults() -> None:
    config = load_snowflake_config()
    assert config.database == "RETAIL_DW"
    assert config.schema == "RAW"
