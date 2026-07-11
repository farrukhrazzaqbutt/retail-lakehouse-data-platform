"""Snowflake Gold load pipeline orchestration."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import SparkSession

from retail_lakehouse.config.settings import SnowflakeConfig, SnowflakeLoadConfig
from retail_lakehouse.utils.helpers import ensure_directory
from retail_lakehouse.warehouse.gold_reader import GoldDeltaReader
from retail_lakehouse.warehouse.loader import SnowflakeLoader

logger = logging.getLogger(__name__)


@dataclass
class SnowflakeTableResult:
    """Summary of a single Snowflake table load."""

    table_name: str
    layer: str
    source_path: str
    row_count: int


@dataclass
class SnowflakeLoadResult:
    """Summary of a complete Snowflake load run."""

    batch_id: str | None
    tables: list[SnowflakeTableResult] = field(default_factory=list)

    @property
    def total_rows(self) -> int:
        """Return total rows loaded across all tables."""
        return sum(table.row_count for table in self.tables)


class SnowflakeLoadPipeline:
    """Orchestrate Gold Delta → Snowflake RAW loads."""

    def __init__(
        self,
        spark: SparkSession,
        snowflake_config: SnowflakeConfig,
        load_config: SnowflakeLoadConfig,
    ) -> None:
        """
        Initialize Snowflake load pipeline.

        Args:
            spark: Active Spark session for reading Gold Delta tables.
            snowflake_config: Snowflake connection settings.
            load_config: Gold load behavior configuration.
        """
        self.spark = spark
        self.snowflake_config = snowflake_config
        self.load_config = load_config
        self.reader = GoldDeltaReader(spark, load_config)
        self.loader = SnowflakeLoader(snowflake_config, load_config)

    def run(
        self,
        batch_id: str | None = None,
        tables: list[str] | None = None,
    ) -> SnowflakeLoadResult:
        """
        Execute the Snowflake load pipeline.

        Args:
            batch_id: Optional batch identifier for manifest logging.
            tables: Optional subset of table names to load.

        Returns:
            SnowflakeLoadResult with per-table summaries.
        """
        result = SnowflakeLoadResult(batch_id=batch_id)
        requested = {name.lower() for name in tables} if tables else None

        for layer, table_name in self.reader.list_configured_tables():
            if requested and table_name.lower() not in requested:
                continue

            if not self.reader.table_exists(layer, table_name):
                raise FileNotFoundError(
                    f"Gold table missing for Snowflake load: {layer}/{table_name}"
                )

            source_path = str(self.reader.table_path(layer, table_name))
            df = self.reader.read_table(layer, table_name)
            self.loader.ensure_table(table_name, df)
            row_count = self.loader.load_dataframe(table_name, df)

            result.tables.append(
                SnowflakeTableResult(
                    table_name=table_name,
                    layer=layer,
                    source_path=source_path,
                    row_count=row_count,
                )
            )

        self._write_manifest(result)
        logger.info(
            "Snowflake load complete tables=%s total_rows=%s",
            len(result.tables),
            result.total_rows,
        )
        return result

    def _write_manifest(self, result: SnowflakeLoadResult) -> None:
        """Persist a JSON manifest describing the Snowflake load run."""
        manifest_dir = ensure_directory(
            self.load_config.manifest_root / self.load_config.manifest_base_path
        )
        suffix = result.batch_id or "latest"
        payload: dict[str, Any] = {
            "batch_id": result.batch_id,
            "database": self.snowflake_config.database,
            "schema": self.snowflake_config.schema,
            "load_mode": self.load_config.load_mode,
            "total_rows": result.total_rows,
            "tables": [
                {
                    "table_name": table.table_name,
                    "layer": table.layer,
                    "source_path": table.source_path,
                    "row_count": table.row_count,
                }
                for table in result.tables
            ],
        }
        manifest_path = manifest_dir / f"snowflake_run_{suffix}.json"
        manifest_path.write_text(
            json.dumps(payload, indent=2),
            encoding="utf-8",
        )
