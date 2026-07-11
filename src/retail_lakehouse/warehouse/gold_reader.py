"""Read Gold Delta tables for Snowflake loading."""

from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from retail_lakehouse.config.settings import SnowflakeLoadConfig

logger = logging.getLogger(__name__)


class GoldDeltaReader:
    """Load Gold Delta tables from the local lakehouse."""

    def __init__(self, spark: SparkSession, config: SnowflakeLoadConfig) -> None:
        """
        Initialize Gold Delta reader.

        Args:
            spark: Active Spark session.
            config: Snowflake load configuration.
        """
        self.spark = spark
        self.config = config

    def read_table(self, layer: str, table_name: str) -> DataFrame:
        """
        Read a Gold Delta table.

        Args:
            layer: Gold layer (dimensions, facts, marts).
            table_name: Table name.

        Returns:
            Gold DataFrame.

        Raises:
            FileNotFoundError: If the Gold Delta table does not exist.
        """
        path = self.table_path(layer, table_name)
        if not path.exists():
            raise FileNotFoundError(f"Gold Delta table not found: {path}")
        logger.info("Reading Gold table=%s path=%s", table_name, path)
        return self.spark.read.format("delta").load(str(path))

    def table_path(self, layer: str, table_name: str) -> Path:
        """Build path to a Gold Delta table."""
        return self.config.gold_root / self.config.gold_base_path / layer / table_name

    def table_exists(self, layer: str, table_name: str) -> bool:
        """Return True when a Gold Delta table exists."""
        path = self.table_path(layer, table_name)
        return path.exists() and (path / "_delta_log").exists()

    def list_configured_tables(self) -> list[tuple[str, str]]:
        """Return all configured (layer, table) pairs in load order."""
        tables: list[tuple[str, str]] = []
        for group in self.config.load_order:
            for table_name in group.tables:
                tables.append((group.layer, table_name))
        return tables
