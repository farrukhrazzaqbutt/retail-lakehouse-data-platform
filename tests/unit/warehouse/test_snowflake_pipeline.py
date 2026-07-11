"""Tests for Snowflake load pipeline."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("pyspark")

from retail_lakehouse.config.settings import (
    SnowflakeConfig,
    SnowflakeLoadLayer,
    load_snowflake_load_config,
)
from retail_lakehouse.warehouse.pipeline import SnowflakeLoadPipeline


def _write_gold_delta(spark, path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    spark.createDataFrame(rows).write.format("delta").mode("overwrite").save(str(path))


def test_snowflake_pipeline_with_mocked_loader(
    spark_session,
    tmp_path,
) -> None:
    load_config = replace(
        load_snowflake_load_config(config_path=Path("config/snowflake_load.yaml")),
        gold_root=tmp_path / "gold",
        manifest_root=tmp_path / "warehouse",
        load_order=[
            SnowflakeLoadLayer(
                layer="dimensions",
                tables=["dim_country"],
            )
        ],
    )

    gold_path = (
        load_config.gold_root
        / load_config.gold_base_path
        / "dimensions"
        / "dim_country"
    )
    _write_gold_delta(
        spark_session,
        gold_path,
        [
            {
                "country_key": "US",
                "country_code": "US",
                "country_name": "United States",
                "gold_loaded_at": "2024-01-01T00:00:00",
            }
        ],
    )

    snowflake_config = SnowflakeConfig(
        account="acct",
        user="user",
        password="pass",
        role="role",
        warehouse="wh",
        database="RETAIL_DW",
        schema="RAW",
    )

    pipeline = SnowflakeLoadPipeline(
        spark_session,
        snowflake_config,
        load_config,
    )

    with (
        patch.object(
            pipeline.loader,
            "ensure_table",
            MagicMock(),
        ),
        patch.object(
            pipeline.loader,
            "load_dataframe",
            return_value=1,
        ) as mock_load,
    ):
        result = pipeline.run(batch_id="snowflake_test_batch")

    assert len(result.tables) == 1
    assert result.tables[0].table_name == "dim_country"
    assert result.total_rows == 1
    mock_load.assert_called_once()

    manifest_path = (
        load_config.manifest_root
        / load_config.manifest_base_path
        / "snowflake_run_snowflake_test_batch.json"
    )
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["batch_id"] == "snowflake_test_batch"
    assert manifest["total_rows"] == 1
