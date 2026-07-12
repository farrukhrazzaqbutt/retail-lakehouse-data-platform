"""Silver and quarantine Delta writers."""

from __future__ import annotations

import logging
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

from retail_lakehouse.config.settings import SilverEntityConfig, SilverTransformConfig
from retail_lakehouse.utils.helpers import ensure_directory

logger = logging.getLogger(__name__)


class SilverWriter:
    """Write validated Silver tables and quarantine datasets to Delta Lake."""

    def __init__(self, spark: SparkSession, config: SilverTransformConfig) -> None:
        """
        Initialize Silver writer.

        Args:
            spark: Active Spark session.
            config: Silver transformation configuration.
        """
        self.spark = spark
        self.config = config

    def write_silver(
        self,
        df: DataFrame,
        entity: SilverEntityConfig,
        batch_id: str | None = None,
    ) -> str:
        """
        Merge valid records into a Silver Delta table.

        Args:
            df: Validated, deduplicated DataFrame.
            entity: Entity configuration.
            batch_id: Optional batch identifier for logging.

        Returns:
            Output path written.
        """
        output_path = self._silver_path(entity)
        ensure_directory(Path(output_path).parent)
        self._merge_delta(
            df, output_path, entity.primary_key, "silver", entity.name, batch_id
        )
        return output_path

    def write_quarantine(
        self,
        df: DataFrame,
        entity: SilverEntityConfig,
        batch_id: str,
        ingestion_date: str,
    ) -> str:
        """
        Append rejected records to a partitioned quarantine Delta table.

        Args:
            df: Quarantine DataFrame with rejection reasons.
            entity: Entity configuration.
            batch_id: Pipeline batch identifier.
            ingestion_date: Partition date.

        Returns:
            Output path written.
        """
        if df.isEmpty():
            logger.info("No quarantine rows for entity=%s", entity.name)
            return ""

        output_path = str(
            self.config.silver_root
            / self.config.quarantine_base_path
            / entity.name
            / f"ingestion_date={ingestion_date}"
            / f"batch_id={batch_id}"
        )
        ensure_directory(Path(output_path))
        row_count = df.count()
        df.write.format("delta").mode("append").save(output_path)
        logger.warning(
            "Quarantine written entity=%s rows=%s path=%s",
            entity.name,
            row_count,
            output_path,
        )
        return output_path

    def _silver_path(self, entity: SilverEntityConfig) -> str:
        """Build Silver Delta table path for an entity."""
        return str(
            self.config.silver_root
            / self.config.silver_base_path
            / entity.source_type
            / entity.name
        )

    def _merge_delta(
        self,
        df: DataFrame,
        output_path: str,
        primary_key: str,
        layer: str,
        entity_name: str,
        batch_id: str | None,
    ) -> None:
        """Create or merge records into a Delta table."""
        if df.isEmpty():
            logger.info("Skipping empty write layer=%s entity=%s", layer, entity_name)
            return

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
            logger.info(
                "Delta MERGE complete layer=%s entity=%s batch_id=%s rows=%s",
                layer,
                entity_name,
                batch_id,
                df.count(),
            )
        else:
            ensure_directory(path)
            df.write.format("delta").mode("overwrite").save(output_path)
            logger.info(
                "Delta CREATE complete layer=%s entity=%s batch_id=%s rows=%s",
                layer,
                entity_name,
                batch_id,
                df.count(),
            )
