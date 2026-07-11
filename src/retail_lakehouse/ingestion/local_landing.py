"""Local and Azurite-backed ADLS landing implementation."""

from __future__ import annotations

import json
import logging
import shutil
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from retail_lakehouse.config.settings import (
    AdfIngestionConfig,
    FileSourceDefinition,
    PostgresSourceTable,
)
from retail_lakehouse.ingestion.metadata import (
    IngestionMetadata,
    build_landing_path,
    enrich_dataframe,
)
from retail_lakehouse.ingestion.postgres_extractor import PostgresExtractor
from retail_lakehouse.utils.helpers import ensure_directory

logger = logging.getLogger(__name__)


@dataclass
class LandingResult:
    """Summary of a single entity landing operation."""

    entity: str
    source_type: str
    rows_landed: int
    landing_path: str
    batch_id: str
    source_system: str


@dataclass
class IngestionRunResult:
    """Summary of a complete ingestion pipeline run."""

    batch_id: str
    ingestion_date: str
    landings: list[LandingResult] = field(default_factory=list)

    @property
    def total_rows(self) -> int:
        """Return total rows landed across all entities."""
        return sum(item.rows_landed for item in self.landings)


class LocalLandingPipeline:
    """
    Mirror Azure Data Factory copy activities for local development.

    Lands PostgreSQL tables and file sources into a partitioned raw zone
    that matches the ADLS path conventions used in production ADF pipelines.
    """

    def __init__(
        self,
        ingestion_config: AdfIngestionConfig,
        postgres_extractor: PostgresExtractor,
        file_sources_dir: Path,
    ) -> None:
        """
        Initialize local landing pipeline.

        Args:
            ingestion_config: ADF ingestion configuration.
            postgres_extractor: PostgreSQL extraction helper.
            file_sources_dir: Directory containing staged CSV/JSON files.
        """
        self.config = ingestion_config
        self.postgres_extractor = postgres_extractor
        self.file_sources_dir = file_sources_dir
        self._run_metadata = IngestionMetadata.create(
            batch_id_prefix=self.config.batch_id_prefix,
            source_system="retail_ingestion_orchestrator",
            source_file="pl_master_ingestion",
        )

    @property
    def run_batch_id(self) -> str:
        """Return the batch identifier for the current pipeline run."""
        return self._run_metadata.batch_id

    def run(
        self,
        *,
        include_postgres: bool = True,
        include_files: bool = True,
        watermark_values: dict[str, datetime] | None = None,
    ) -> IngestionRunResult:
        """
        Execute the full local ingestion pipeline.

        Args:
            include_postgres: Whether to ingest PostgreSQL tables.
            include_files: Whether to ingest file sources.
            watermark_values: Optional incremental watermarks per entity.

        Returns:
            IngestionRunResult with per-entity landing summaries.
        """
        result = IngestionRunResult(
            batch_id=self._run_metadata.batch_id,
            ingestion_date=self._run_metadata.ingestion_date.isoformat(),
        )
        watermarks = watermark_values or {}

        if include_postgres:
            for table in self.config.postgres_tables:
                landing = self._land_postgres_table(
                    table,
                    watermark=watermarks.get(table.entity),
                )
                result.landings.append(landing)

        if include_files:
            for source in self.config.file_sources:
                landing = self._land_file_source(source)
                result.landings.append(landing)

        self._write_manifest(result)
        logger.info(
            "Ingestion complete batch_id=%s total_rows=%s entities=%s",
            result.batch_id,
            result.total_rows,
            len(result.landings),
        )
        return result

    def _land_postgres_table(
        self,
        table: PostgresSourceTable,
        watermark: datetime | None = None,
    ) -> LandingResult:
        """Extract and land a PostgreSQL table as Parquet."""
        metadata = IngestionMetadata.create(
            batch_id_prefix=self.config.batch_id_prefix,
            source_system=self.config.postgres_source_system,
            source_file=f"{self.config.postgres_schema}.{table.entity}",
            ingestion_date=self._run_metadata.ingestion_date,
        )
        metadata = IngestionMetadata(
            batch_id=self._run_metadata.batch_id,
            source_system=metadata.source_system,
            source_file=metadata.source_file,
            ingested_at=metadata.ingested_at,
            ingestion_date=metadata.ingestion_date,
        )

        df = self.postgres_extractor.extract_table(table, watermark_value=watermark)
        enriched = enrich_dataframe(df, metadata)
        relative_path = build_landing_path(
            self.config.path_template,
            base_path=self.config.adls_base_path,
            source_type="postgres",
            entity=table.entity,
            metadata=metadata,
        )
        output_dir = ensure_directory(self.config.local_landing_dir / relative_path)
        output_file = output_dir / f"{table.entity}.parquet"
        enriched.to_parquet(output_file, index=False)

        return LandingResult(
            entity=table.entity,
            source_type="postgres",
            rows_landed=len(enriched),
            landing_path=str(output_file.relative_to(self.config.local_landing_dir)),
            batch_id=metadata.batch_id,
            source_system=metadata.source_system,
        )

    def _land_file_source(self, source: FileSourceDefinition) -> LandingResult:
        """Copy and enrich file sources into the raw landing zone."""
        metadata = IngestionMetadata(
            batch_id=self._run_metadata.batch_id,
            source_system=source.source_system,
            source_file=source.entity,
            ingested_at=datetime.now(UTC),
            ingestion_date=self._run_metadata.ingestion_date,
        )
        source_dir = self.file_sources_dir / source.entity
        if not source_dir.exists():
            raise FileNotFoundError(
                f"File source directory not found: {source_dir}. "
                "Run scripts/generate_file_sources.py first."
            )

        files = sorted(source_dir.glob(source.pattern))
        if not files:
            raise FileNotFoundError(
                f"No files matching {source.pattern} in {source_dir}"
            )

        relative_path = build_landing_path(
            self.config.path_template,
            base_path=self.config.adls_base_path,
            source_type="file",
            entity=source.entity,
            metadata=metadata,
        )
        output_dir = ensure_directory(self.config.local_landing_dir / relative_path)

        rows_landed = 0
        for src_file in files:
            metadata = IngestionMetadata(
                batch_id=self._run_metadata.batch_id,
                source_system=source.source_system,
                source_file=src_file.name,
                ingested_at=datetime.now(UTC),
                ingestion_date=self._run_metadata.ingestion_date,
            )
            dest_name = src_file.stem + "_enriched" + src_file.suffix
            dest_path = output_dir / dest_name

            if source.file_format == "csv":
                df = pd.read_csv(src_file)
                enriched = enrich_dataframe(df, metadata)
                enriched.to_csv(dest_path, index=False)
                rows_landed += len(enriched)
            elif source.file_format == "json":
                records = self._read_json_lines(src_file)
                for record in records:
                    record.update(metadata.as_dict())
                dest_path = output_dir / (src_file.stem + "_enriched.json")
                with dest_path.open("w", encoding="utf-8") as handle:
                    json.dump(records, handle, indent=2, default=str)
                rows_landed += len(records)
            else:
                shutil.copy2(src_file, output_dir / src_file.name)
                rows_landed += 1

        return LandingResult(
            entity=source.entity,
            source_type="file",
            rows_landed=rows_landed,
            landing_path=str(output_dir.relative_to(self.config.local_landing_dir)),
            batch_id=self._run_metadata.batch_id,
            source_system=source.source_system,
        )

    def _write_manifest(self, result: IngestionRunResult) -> None:
        """Write a JSON manifest describing the ingestion run."""
        manifest_dir = ensure_directory(
            self.config.local_landing_dir
            / self.config.adls_base_path
            / "_manifests"
            / f"ingestion_date={result.ingestion_date}"
        )
        manifest_path = manifest_dir / f"batch_id={result.batch_id}.json"
        payload: dict[str, Any] = {
            "batch_id": result.batch_id,
            "ingestion_date": result.ingestion_date,
            "total_rows": result.total_rows,
            "landings": [
                {
                    "entity": landing.entity,
                    "source_type": landing.source_type,
                    "rows_landed": landing.rows_landed,
                    "landing_path": landing.landing_path,
                    "source_system": landing.source_system,
                }
                for landing in result.landings
            ],
        }
        with manifest_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)

    @staticmethod
    def _read_json_lines(path: Path) -> list[dict[str, Any]]:
        """Read JSON array or newline-delimited JSON file."""
        content = path.read_text(encoding="utf-8").strip()
        if not content:
            return []
        if content.startswith("["):
            data = json.loads(content)
            return list(data) if isinstance(data, list) else [data]
        return [json.loads(line) for line in content.splitlines() if line.strip()]
