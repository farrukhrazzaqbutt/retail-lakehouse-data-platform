"""End-to-end pipeline reconciliation reporting."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text

from retail_lakehouse.config.settings import (
    PostgresConfig,
    load_adf_ingestion_config,
    load_gold_model_config,
    load_postgres_config,
    load_silver_transform_config,
    load_snowflake_load_config,
)

logger = logging.getLogger(__name__)

POSTGRES_ENTITIES = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
]


@dataclass
class LayerCount:
    """Row count summary for a single entity in a layer."""

    entity: str
    row_count: int | None
    status: str
    detail: str = ""


@dataclass
class ReconciliationResult:
    """Full reconciliation report across pipeline layers."""

    generated_at: str
    postgres_counts: list[LayerCount] = field(default_factory=list)
    bronze_counts: list[LayerCount] = field(default_factory=list)
    silver_counts: list[LayerCount] = field(default_factory=list)
    gold_counts: list[LayerCount] = field(default_factory=list)
    snowflake_counts: list[LayerCount] = field(default_factory=list)
    issues: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        """Return True when no reconciliation issues were found."""
        return len(self.issues) == 0

    def to_dict(self) -> dict[str, Any]:
        """Serialize the reconciliation result to a dictionary."""
        return {
            "generated_at": self.generated_at,
            "passed": self.passed,
            "issues": self.issues,
            "postgres": [layer.__dict__ for layer in self.postgres_counts],
            "bronze": [layer.__dict__ for layer in self.bronze_counts],
            "silver": [layer.__dict__ for layer in self.silver_counts],
            "gold": [layer.__dict__ for layer in self.gold_counts],
            "snowflake": [layer.__dict__ for layer in self.snowflake_counts],
        }


class ReconciliationReporter:
    """Collect row counts across Postgres, lakehouse manifests, and Snowflake."""

    def __init__(
        self,
        postgres_config: PostgresConfig | None = None,
        output_dir: Path | None = None,
    ) -> None:
        self.postgres_config = postgres_config or load_postgres_config()
        self.ingestion_config = load_adf_ingestion_config()
        self.silver_config = load_silver_transform_config()
        self.gold_config = load_gold_model_config()
        self.snowflake_load_config = load_snowflake_load_config()
        self.output_dir = output_dir or Path("data/reconciliation")

    def run(self, skip_snowflake: bool = False) -> ReconciliationResult:
        """
        Execute reconciliation across available pipeline layers.

        Args:
            skip_snowflake: Skip Snowflake row count checks.

        Returns:
            ReconciliationResult with per-layer counts and issues.
        """
        result = ReconciliationResult(generated_at=datetime.now(UTC).isoformat())

        result.postgres_counts = self._postgres_counts()
        result.bronze_counts = self._bronze_counts()
        result.silver_counts = self._manifest_counts(
            self.silver_config.silver_root
            / self.silver_config.silver_base_path
            / "_manifests",
            "silver",
        )
        result.gold_counts = self._manifest_counts(
            self.gold_config.gold_root / self.gold_config.gold_base_path / "_manifests",
            "gold",
        )
        result.snowflake_counts = (
            []
            if skip_snowflake
            else self._manifest_counts(
                self.snowflake_load_config.manifest_root
                / self.snowflake_load_config.manifest_base_path,
                "snowflake",
                prefix="snowflake_run_",
            )
        )

        self._compare_layers(result)
        return result

    def write_report(self, result: ReconciliationResult) -> Path:
        """Persist reconciliation output as JSON."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = result.generated_at.replace(":", "-")
        path = self.output_dir / f"reconciliation_{timestamp}.json"
        path.write_text(json.dumps(result.to_dict(), indent=2), encoding="utf-8")
        logger.info("Wrote reconciliation report path=%s", path)
        return path

    def _postgres_counts(self) -> list[LayerCount]:
        """Read PostgreSQL entity row counts."""
        counts: list[LayerCount] = []
        try:
            engine = create_engine(self.postgres_config.sqlalchemy_url)
            with engine.connect() as connection:
                for entity in POSTGRES_ENTITIES:
                    query = text(
                        f"SELECT COUNT(*) FROM {self.postgres_config.schema}.{entity}"
                    )
                    row_count = int(connection.execute(query).scalar_one())
                    counts.append(
                        LayerCount(
                            entity=entity,
                            row_count=row_count,
                            status="ok",
                        )
                    )
        except Exception as exc:
            counts.append(
                LayerCount(
                    entity="postgres",
                    row_count=None,
                    status="error",
                    detail=str(exc),
                )
            )
        return counts

    def _bronze_counts(self) -> list[LayerCount]:
        """Estimate Bronze entity row counts from latest parquet partitions."""
        counts: list[LayerCount] = []
        bronze_root = (
            self.ingestion_config.local_landing_dir
            / self.ingestion_config.adls_base_path
            / "postgres"
        )
        if not bronze_root.exists():
            return [
                LayerCount(
                    entity="bronze",
                    row_count=None,
                    status="missing",
                    detail=f"Bronze root not found: {bronze_root}",
                )
            ]

        try:
            import pandas as pd
        except ImportError:
            return [
                LayerCount(
                    entity="bronze",
                    row_count=None,
                    status="skipped",
                    detail="pandas not available",
                )
            ]

        for entity in POSTGRES_ENTITIES:
            entity_root = bronze_root / entity
            if not entity_root.exists():
                counts.append(
                    LayerCount(
                        entity=entity,
                        row_count=None,
                        status="missing",
                    )
                )
                continue
            parquet_files = list(entity_root.glob("**/*.parquet"))
            if not parquet_files:
                counts.append(
                    LayerCount(
                        entity=entity,
                        row_count=0,
                        status="empty",
                    )
                )
                continue
            frames = [pd.read_parquet(path) for path in parquet_files[:20]]
            row_count = sum(len(frame) for frame in frames)
            counts.append(
                LayerCount(
                    entity=entity,
                    row_count=row_count,
                    status="ok",
                    detail=f"sampled_files={min(len(parquet_files), 20)}",
                )
            )
        return counts

    def _manifest_counts(
        self,
        manifest_dir: Path,
        layer_name: str,
        prefix: str = "",
    ) -> list[LayerCount]:
        """Read row counts from the latest layer manifest JSON."""
        if not manifest_dir.exists():
            return [
                LayerCount(
                    entity=layer_name,
                    row_count=None,
                    status="missing",
                    detail=f"No manifest directory: {manifest_dir}",
                )
            ]

        pattern = f"{prefix}*.json" if prefix else "*.json"
        manifests = list(manifest_dir.glob(pattern))
        if not manifests and prefix:
            manifests = list(manifest_dir.glob("**/*.json"))
        if not manifests:
            return [
                LayerCount(
                    entity=layer_name,
                    row_count=None,
                    status="missing",
                    detail="No manifests found",
                )
            ]

        latest = max(manifests, key=lambda path: path.stat().st_mtime)
        payload = json.loads(latest.read_text(encoding="utf-8"))
        tables = payload.get("tables", [])
        counts = [
            LayerCount(
                entity=str(table.get("table_name", table.get("entity", "unknown"))),
                row_count=int(table.get("row_count", 0)),
                status="ok",
                detail=f"manifest={latest.name}",
            )
            for table in tables
        ]
        if not counts and "total_rows" in payload:
            counts.append(
                LayerCount(
                    entity=layer_name,
                    row_count=int(payload["total_rows"]),
                    status="ok",
                    detail=f"manifest={latest.name}",
                )
            )
        return counts

    def _compare_layers(self, result: ReconciliationResult) -> None:
        """Compare Postgres and Bronze counts and record mismatches."""
        postgres_map = {
            layer.entity: layer.row_count
            for layer in result.postgres_counts
            if layer.row_count is not None
        }
        bronze_map = {
            layer.entity: layer.row_count
            for layer in result.bronze_counts
            if layer.row_count is not None and layer.status == "ok"
        }

        for entity, pg_count in postgres_map.items():
            bronze_count = bronze_map.get(entity)
            if bronze_count is None:
                result.issues.append(f"{entity}: bronze count unavailable")
            elif pg_count != bronze_count:
                result.issues.append(
                    f"{entity}: postgres={pg_count} bronze={bronze_count}"
                )

        for layer in result.postgres_counts + result.bronze_counts:
            if layer.status in {"error", "missing"} and layer.entity != "bronze":
                result.issues.append(
                    f"{layer.entity}: {layer.status} {layer.detail}".strip()
                )
