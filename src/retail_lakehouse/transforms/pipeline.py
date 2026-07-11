"""Silver transformation pipeline orchestration."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from pyspark.sql import DataFrame, SparkSession

from retail_lakehouse.config.settings import (
    SilverEntityConfig,
    SilverTransformConfig,
)
from retail_lakehouse.transforms.bronze_reader import BronzeReader
from retail_lakehouse.transforms.dedupe import deduplicate_latest
from retail_lakehouse.transforms.quality import DataQualityEngine
from retail_lakehouse.transforms.silver_writer import SilverWriter
from retail_lakehouse.utils.helpers import ensure_directory

logger = logging.getLogger(__name__)

# Processing order respects foreign-key dependencies
ENTITY_PROCESS_ORDER: list[str] = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "product_updates",
    "website_events",
]


@dataclass
class EntityTransformResult:
    """Summary of a single entity Silver transformation."""

    entity: str
    valid_rows: int
    quarantine_rows: int
    silver_path: str
    quarantine_path: str


@dataclass
class SilverPipelineResult:
    """Summary of a complete Silver pipeline run."""

    batch_id: str
    ingestion_date: str
    entities: list[EntityTransformResult] = field(default_factory=list)

    @property
    def total_valid_rows(self) -> int:
        """Return total valid rows written across entities."""
        return sum(item.valid_rows for item in self.entities)

    @property
    def total_quarantine_rows(self) -> int:
        """Return total quarantined rows across entities."""
        return sum(item.quarantine_rows for item in self.entities)


class SilverTransformPipeline:
    """Orchestrate Bronze → Silver transforms with quarantine handling."""

    def __init__(
        self,
        spark: SparkSession,
        config: SilverTransformConfig,
    ) -> None:
        """
        Initialize Silver pipeline.

        Args:
            spark: Active Spark session.
            config: Silver transformation configuration.
        """
        self.spark = spark
        self.config = config
        self.reader = BronzeReader(spark, config)
        self.quality = DataQualityEngine(config)
        self.writer = SilverWriter(spark, config)
        self._entity_map = {entity.name: entity for entity in config.entities}
        self._silver_cache: dict[str, DataFrame] = {}

    def run(
        self,
        batch_id: str | None = None,
        ingestion_date: str | None = None,
        entities: list[str] | None = None,
    ) -> SilverPipelineResult:
        """
        Execute Silver transforms for configured entities.

        Args:
            batch_id: Optional batch identifier (inferred from Bronze if omitted).
            ingestion_date: Optional partition date (inferred if omitted).
            entities: Optional subset of entities to process.

        Returns:
            SilverPipelineResult with per-entity summaries.
        """
        target_entities = entities or ENTITY_PROCESS_ORDER
        resolved_batch_id, resolved_date = self._resolve_run_identifiers(
            batch_id, ingestion_date
        )

        result = SilverPipelineResult(
            batch_id=resolved_batch_id,
            ingestion_date=resolved_date,
        )

        for entity_name in target_entities:
            entity = self._entity_map.get(entity_name)
            if entity is None:
                raise KeyError(f"Unknown entity: {entity_name}")
            if not self.reader.bronze_exists(entity.name, entity.source_type):
                logger.warning("Skipping entity=%s — no Bronze data found", entity.name)
                continue

            entity_result = self._transform_entity(
                entity,
                batch_id=resolved_batch_id,
                ingestion_date=resolved_date,
            )
            result.entities.append(entity_result)

        self._write_manifest(result)
        logger.info(
            "Silver pipeline complete batch_id=%s valid=%s quarantine=%s",
            resolved_batch_id,
            result.total_valid_rows,
            result.total_quarantine_rows,
        )
        return result

    def _transform_entity(
        self,
        entity: SilverEntityConfig,
        *,
        batch_id: str,
        ingestion_date: str,
    ) -> EntityTransformResult:
        """Transform a single entity from Bronze to Silver."""
        bronze_df = self.reader.read_entity(entity)
        valid_df, quarantine_df = self.quality.validate_entity(bronze_df, entity)

        if self.config.referential_integrity_enabled:
            valid_df, quarantine_df = self._apply_foreign_key_checks(
                entity, valid_df, quarantine_df
            )

        valid_df = deduplicate_latest(valid_df, entity, self.config)
        silver_path = self.writer.write_silver(valid_df, entity, batch_id=batch_id)
        quarantine_path = self.writer.write_quarantine(
            quarantine_df, entity, batch_id=batch_id, ingestion_date=ingestion_date
        )

        valid_rows = valid_df.count()
        quarantine_rows = quarantine_df.count()
        self._silver_cache[entity.name] = valid_df

        return EntityTransformResult(
            entity=entity.name,
            valid_rows=valid_rows,
            quarantine_rows=quarantine_rows,
            silver_path=silver_path,
            quarantine_path=quarantine_path,
        )

    def _apply_foreign_key_checks(
        self,
        entity: SilverEntityConfig,
        valid_df: DataFrame,
        quarantine_df: DataFrame,
    ) -> tuple[DataFrame, DataFrame]:
        """Apply referential integrity checks using already-processed Silver parents."""
        checks = [
            check
            for check in self.config.referential_integrity_checks
            if check.child_entity == entity.name
        ]
        for check in checks:
            parent_df = self._silver_cache.get(check.parent_entity)
            if parent_df is None:
                logger.warning(
                    "Skipping FK check child=%s parent=%s — parent not yet processed",
                    entity.name,
                    check.parent_entity,
                )
                continue
            valid_df, quarantine_df = self.quality.apply_referential_integrity(
                valid_df,
                quarantine_df,
                entity,
                parent_df,
                check,
            )
        return valid_df, quarantine_df

    def _resolve_run_identifiers(
        self,
        batch_id: str | None,
        ingestion_date: str | None,
    ) -> tuple[str, str]:
        """Infer batch_id and ingestion_date from Bronze manifests when not provided."""
        if batch_id and ingestion_date:
            return batch_id, ingestion_date

        manifest_dir = (
            self.config.bronze_root / self.config.bronze_base_path / "_manifests"
        )
        manifests = sorted(
            manifest_dir.glob("**/*.json"), key=lambda p: p.stat().st_mtime
        )
        if not manifests:
            raise FileNotFoundError(
                f"No Bronze manifests found under {manifest_dir}. Run Phase 2 ingestion first."
            )

        latest = json.loads(manifests[-1].read_text(encoding="utf-8"))
        resolved_batch = batch_id or str(latest["batch_id"])
        resolved_date = ingestion_date or str(latest["ingestion_date"])
        return resolved_batch, resolved_date

    def _write_manifest(self, result: SilverPipelineResult) -> None:
        """Persist a JSON manifest describing the Silver run."""
        manifest_dir = ensure_directory(
            self.config.silver_root
            / self.config.silver_base_path
            / "_manifests"
            / f"ingestion_date={result.ingestion_date}"
        )
        payload: dict[str, Any] = {
            "batch_id": result.batch_id,
            "ingestion_date": result.ingestion_date,
            "total_valid_rows": result.total_valid_rows,
            "total_quarantine_rows": result.total_quarantine_rows,
            "entities": [
                {
                    "entity": entity.entity,
                    "valid_rows": entity.valid_rows,
                    "quarantine_rows": entity.quarantine_rows,
                    "silver_path": entity.silver_path,
                    "quarantine_path": entity.quarantine_path,
                }
                for entity in result.entities
            ],
        }
        manifest_path = manifest_dir / f"batch_id={result.batch_id}.json"
        manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
