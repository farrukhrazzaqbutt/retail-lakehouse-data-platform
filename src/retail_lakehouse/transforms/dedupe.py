"""Deduplication utilities for Silver transforms."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from retail_lakehouse.config.settings import SilverEntityConfig, SilverTransformConfig


def deduplicate_latest(
    df: DataFrame,
    entity: SilverEntityConfig,
    config: SilverTransformConfig,
) -> DataFrame:
    """
    Keep the latest record per dedupe key using configured ordering columns.

    Args:
        df: Validated entity DataFrame.
        entity: Entity configuration with dedupe keys.
        config: Silver configuration with order columns.

    Returns:
        Deduplicated DataFrame.
    """
    if df.rdd.isEmpty():
        return df

    order_cols = [col for col in config.dedupe_order_columns if col in df.columns]
    if not order_cols:
        order_cols = [config.processed_at_column]

    window = Window.partitionBy(*entity.dedupe_keys).orderBy(
        *[F.col(col).desc() for col in order_cols]
    )
    return (
        df.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )
