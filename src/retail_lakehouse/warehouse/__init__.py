"""Snowflake warehouse loading from Gold Delta tables."""

from retail_lakehouse.warehouse.pipeline import (
    SnowflakeLoadPipeline,
    SnowflakeLoadResult,
)

__all__ = ["SnowflakeLoadPipeline", "SnowflakeLoadResult"]
