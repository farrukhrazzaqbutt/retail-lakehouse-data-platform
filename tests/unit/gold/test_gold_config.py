"""Tests for Gold model configuration."""

from __future__ import annotations

from pathlib import Path

from retail_lakehouse.config.settings import load_gold_model_config


def test_load_gold_model_config() -> None:
    config = load_gold_model_config(config_path=Path("config/gold_models.yaml"))
    assert len(config.dimensions) == 4
    assert len(config.facts) == 3
    assert len(config.marts) == 5
    assert "dim_date" in config.dimensions
    assert "fct_orders" in config.facts
    assert "mart_daily_sales" in config.marts
    assert config.processed_at_column == "gold_loaded_at"
    assert config.date_start == "2023-01-01"
    assert "completed" in config.completed_statuses
