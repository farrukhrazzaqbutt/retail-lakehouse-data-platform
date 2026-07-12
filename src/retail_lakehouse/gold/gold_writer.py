"""Gold Delta table writer with MERGE support."""

from __future__ import annotations

import logging
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from retail_lakehouse.config.settings import GoldModelConfig
from retail_lakehouse.utils.helpers import ensure_directory

logger = logging.getLogger(__name__)


class GoldWriter:
    """Write Gold dimensions, facts, and marts to Delta Lake."""

    def __init__(self, spark: SparkSession, config: GoldModelConfig) -> None:
        """
        Initialize Gold writer.

        Args:
            spark: Active Spark session.
            config: Gold model configuration.
        """
        self.spark = spark
        self.config = config

    def write_table(
        self,
        df: DataFrame,
        table_name: str,
        primary_key: str | None = None,
        layer: str = "gold",
        mode: str = "merge",
    ) -> str:
        """
        Write a Gold table using merge or overwrite semantics.

        Args:
            df: DataFrame to write.
            table_name: Target table name.
            primary_key: Merge key (required when mode=merge).
            layer: Subdirectory layer (dimensions, facts, marts).
            mode: ``merge`` or ``overwrite``.

        Returns:
            Output path written.
        """
        output_path = self._gold_path(layer, table_name)
        if df.isEmpty():
            logger.info("Skipping empty Gold write table=%s", table_name)
            return str(output_path)

        if mode == "merge" and primary_key:
            self._merge_delta(df, str(output_path), primary_key, table_name)
        else:
            ensure_directory(output_path)
            df.write.format("delta").mode("overwrite").save(str(output_path))
            logger.info("Gold OVERWRITE table=%s rows=%s", table_name, df.count())

        return str(output_path)

    def _gold_path(self, layer: str, table_name: str) -> Path:
        """Build Gold Delta table path."""
        return self.config.gold_root / self.config.gold_base_path / layer / table_name

    def _merge_delta(
        self,
        df: DataFrame,
        output_path: str,
        primary_key: str,
        table_name: str,
    ) -> None:
        """Create or merge records into a Gold Delta table."""
        path = Path(output_path)
        if path.exists() and (path / "_delta_log").exists():
            delta_table = DeltaTable.forPath(self.spark, output_path)
            (
                delta_table.alias("target")
                .merge(
                    df.alias("source"),
                    f"target.{primary_key} = source.{primary_key}",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            logger.info("Gold MERGE table=%s rows=%s", table_name, df.count())
        else:
            ensure_directory(path)
            df.write.format("delta").mode("overwrite").save(output_path)
            logger.info("Gold CREATE table=%s rows=%s", table_name, df.count())
