"""Base generator utilities shared across entity generators."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import UTC, datetime

import numpy as np
import pandas as pd
from faker import Faker

from retail_lakehouse.config.settings import DataGenerationConfig


class BaseGenerator(ABC):
    """Abstract base class for entity-specific synthetic data generators."""

    def __init__(self, config: DataGenerationConfig) -> None:
        """
        Initialize generator with shared configuration and RNG state.

        Args:
            config: Data generation configuration.
        """
        self.config = config
        self.rng = np.random.default_rng(config.seed)
        self.faker = Faker()
        Faker.seed(config.seed)

    def audit_timestamp(self) -> datetime:
        """Return a deterministic UTC timestamp derived from the configured seed."""
        base = datetime(2024, 1, 1, tzinfo=UTC)
        return base.replace(second=int(self.config.seed % 60))

    def _add_audit_columns(
        self, df: pd.DataFrame, include_updated_at: bool = True
    ) -> pd.DataFrame:
        """
        Append standard audit columns used by the PostgreSQL schema.

        Args:
            df: Entity DataFrame.
            include_updated_at: Whether to include ``updated_at`` column.

        Returns:
            DataFrame with audit columns appended.
        """
        now = self.audit_timestamp()
        result = df.copy()
        result["created_at"] = now
        if include_updated_at:
            result["updated_at"] = now
        result["source_system"] = self.config.source_system
        return result

    @abstractmethod
    def generate(self) -> pd.DataFrame:
        """Generate a DataFrame for the target entity."""
        raise NotImplementedError
