"""Ingestion metadata helpers for Bronze-bound raw landing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from uuid import uuid4

import pandas as pd


@dataclass(frozen=True)
class IngestionMetadata:
    """Standard ingestion metadata appended to every landed dataset."""

    batch_id: str
    source_system: str
    source_file: str
    ingested_at: datetime
    ingestion_date: date

    @classmethod
    def create(
        cls,
        *,
        batch_id_prefix: str,
        source_system: str,
        source_file: str,
        ingestion_date: date | None = None,
    ) -> IngestionMetadata:
        """
        Create ingestion metadata for a pipeline run.

        Args:
            batch_id_prefix: Prefix used in batch identifiers.
            source_system: Upstream system label.
            source_file: Source object name or table identifier.
            ingestion_date: Optional partition date (defaults to UTC today).

        Returns:
            IngestionMetadata instance.
        """
        now = datetime.now(UTC)
        return cls(
            batch_id=f"{batch_id_prefix}_{uuid4().hex[:12]}",
            source_system=source_system,
            source_file=source_file,
            ingested_at=now,
            ingestion_date=ingestion_date or now.date(),
        )

    def as_dict(self) -> dict[str, str]:
        """Return metadata fields as a string dictionary."""
        return {
            "batch_id": self.batch_id,
            "source_system": self.source_system,
            "source_file": self.source_file,
            "ingested_at": self.ingested_at.isoformat(),
            "ingestion_date": self.ingestion_date.isoformat(),
        }


def enrich_dataframe(
    df: pd.DataFrame,
    metadata: IngestionMetadata,
) -> pd.DataFrame:
    """
    Append ingestion metadata columns to a DataFrame.

    Args:
        df: Source records.
        metadata: Ingestion metadata for the batch.

    Returns:
        DataFrame with metadata columns appended.
    """
    result = df.copy()
    for key, value in metadata.as_dict().items():
        result[key] = value
    return result


def build_landing_path(
    template: str,
    *,
    base_path: str,
    source_type: str,
    entity: str,
    metadata: IngestionMetadata,
) -> str:
    """
    Build a partitioned ADLS landing path from the configured template.

    Args:
        template: Path template with format placeholders.
        base_path: Bronze base path segment.
        source_type: Source category (postgres, file).
        entity: Entity or table name.
        metadata: Ingestion metadata for partition values.

    Returns:
        Relative landing path without filesystem prefix.
    """
    return template.format(
        base_path=base_path,
        source_type=source_type,
        entity=entity,
        ingestion_date=metadata.ingestion_date.isoformat(),
        batch_id=metadata.batch_id,
    )
