"""Read Silver Delta tables for Gold model building."""

from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from retail_lakehouse.config.settings import GoldModelConfig

logger = logging.getLogger(__name__)

# Silver postgres entities used by Gold layer
SILVER_POSTGRES_ENTITIES: list[str] = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
]


class SilverReader:
    """Load curated Silver Delta tables."""

    def __init__(self, spark: SparkSession, config: GoldModelConfig) -> None:
        """
        Initialize Silver reader.

        Args:
            spark: Active Spark session.
            config: Gold model configuration.
        """
        self.spark = spark
        self.config = config

    def read_postgres_entity(self, entity: str) -> DataFrame:
        """
        Read a Silver postgres entity Delta table.

        Args:
            entity: Entity name (e.g. customers, orders).

        Returns:
            Silver DataFrame.

        Raises:
            FileNotFoundError: If the Silver table does not exist.
        """
        path = self._silver_path("postgres", entity)
        if not path.exists():
            raise FileNotFoundError(f"Silver table not found: {path}")
        logger.info("Reading Silver entity=%s path=%s", entity, path)
        return self.spark.read.format("delta").load(str(path))

    def read_all_postgres_entities(self) -> dict[str, DataFrame]:
        """Read all Silver postgres entities required for Gold models."""
        return {
            entity: self.read_postgres_entity(entity)
            for entity in SILVER_POSTGRES_ENTITIES
        }

    def silver_exists(self, entity: str) -> bool:
        """Return True when a Silver postgres table exists."""
        path = self._silver_path("postgres", entity)
        return path.exists() and (path / "_delta_log").exists()

    def _silver_path(self, source_type: str, entity: str) -> Path:
        """Build path to a Silver Delta table."""
        return (
            self.config.silver_root
            / self.config.silver_base_path
            / source_type
            / entity
        )
