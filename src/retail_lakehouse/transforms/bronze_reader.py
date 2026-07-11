"""Read Bronze-layer datasets from the local lakehouse mirror."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession

from retail_lakehouse.config.settings import SilverEntityConfig, SilverTransformConfig

logger = logging.getLogger(__name__)


class BronzeReader:
    """Load Bronze entities from partitioned lakehouse paths."""

    def __init__(self, spark: SparkSession, config: SilverTransformConfig) -> None:
        """
        Initialize Bronze reader.

        Args:
            spark: Active Spark session.
            config: Silver transformation configuration.
        """
        self.spark = spark
        self.config = config

    def read_entity(self, entity: SilverEntityConfig) -> DataFrame:
        """
        Read all Bronze files for an entity.

        Args:
            entity: Entity configuration.

        Returns:
            Combined Bronze DataFrame.

        Raises:
            FileNotFoundError: If no Bronze data exists for the entity.
        """
        bronze_path = (
            self.config.bronze_root
            / self.config.bronze_base_path
            / entity.source_type
            / entity.name
        )
        if not bronze_path.exists():
            raise FileNotFoundError(f"Bronze path not found: {bronze_path}")

        path_str = str(bronze_path)
        logger.info(
            "Reading Bronze entity=%s path=%s format=%s",
            entity.name,
            path_str,
            entity.file_format,
        )

        if entity.file_format == "parquet":
            return self.spark.read.parquet(path_str)
        if entity.file_format == "csv":
            return (
                self.spark.read.option("header", True)
                .option("inferSchema", True)
                .csv(str(bronze_path / "**" / "*_enriched.csv"))
            )
        if entity.file_format == "json":
            return self.spark.read.option("multiline", True).json(
                str(bronze_path / "**" / "*_enriched.json")
            )

        raise ValueError(f"Unsupported Bronze format: {entity.file_format}")

    def bronze_exists(self, entity_name: str, source_type: str) -> bool:
        """Return True when Bronze data exists for an entity."""
        path = (
            self.config.bronze_root
            / self.config.bronze_base_path
            / source_type
            / entity_name
        )
        return path.exists() and any(path.rglob("*"))
