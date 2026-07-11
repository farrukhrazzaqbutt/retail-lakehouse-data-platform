"""Data quality validation and quarantine routing for Silver transforms."""

from __future__ import annotations

import logging
from functools import reduce

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from retail_lakehouse.config.settings import (
    NumericConstraint,
    ReferentialIntegrityCheck,
    SilverEntityConfig,
    SilverTransformConfig,
)

logger = logging.getLogger(__name__)


class DataQualityEngine:
    """Apply validation rules and split records into valid vs quarantine sets."""

    def __init__(self, config: SilverTransformConfig) -> None:
        """Initialize engine with Silver configuration."""
        self.config = config

    def validate_entity(
        self,
        df: DataFrame,
        entity: SilverEntityConfig,
    ) -> tuple[DataFrame, DataFrame]:
        """
        Validate an entity DataFrame and split valid/quarantine rows.

        Args:
            df: Bronze entity DataFrame.
            entity: Entity validation configuration.

        Returns:
            Tuple of (valid_df, quarantine_df).
        """
        working = self._standardize_columns(df)
        reasons = self._build_failure_reasons(working, entity)

        quarantine = working.withColumn(
            self.config.rejection_reason_column, reasons
        ).filter(F.col(self.config.rejection_reason_column) != "")
        valid = working.withColumn(
            self.config.rejection_reason_column, F.lit(None)
        ).filter(~self._has_failures(reasons))
        return valid, quarantine

    def apply_referential_integrity(
        self,
        valid_df: DataFrame,
        quarantine_df: DataFrame,
        entity: SilverEntityConfig,
        parent_df: DataFrame,
        check: ReferentialIntegrityCheck,
    ) -> tuple[DataFrame, DataFrame]:
        """
        Move orphan foreign-key rows from valid to quarantine.

        Args:
            valid_df: Current valid records.
            quarantine_df: Current quarantine records.
            entity: Child entity configuration.
            parent_df: Parent Silver DataFrame.
            check: Referential integrity rule.

        Returns:
            Updated (valid_df, quarantine_df).
        """
        parent_keys = parent_df.select(check.parent_column).distinct()
        orphans = valid_df.join(
            parent_keys,
            valid_df[check.child_column] == parent_keys[check.parent_column],
            how="left_anti",
        )
        if orphans.rdd.isEmpty():
            return valid_df, quarantine_df

        reason = f"orphan_{check.child_column}_references_{check.parent_entity}.{check.parent_column}"
        orphan_quarantine = orphans.withColumn(
            self.config.rejection_reason_column, F.lit(reason)
        )
        remaining_valid = valid_df.join(
            orphans.select(entity.primary_key),
            on=entity.primary_key,
            how="left_anti",
        )
        combined_quarantine = quarantine_df.unionByName(
            orphan_quarantine, allowMissingColumns=True
        )
        logger.warning(
            "Referential integrity violation entity=%s reason=%s count=%s",
            entity.name,
            reason,
            orphan_quarantine.count(),
        )
        return remaining_valid, combined_quarantine

    def _standardize_columns(self, df: DataFrame) -> DataFrame:
        """Trim string columns and add processed timestamp."""
        result = df
        for field in result.schema.fields:
            if field.dataType.simpleString() == "string":
                result = result.withColumn(field.name, F.trim(F.col(field.name)))
        return result.withColumn(
            self.config.processed_at_column,
            F.current_timestamp(),
        )

    def _build_failure_reasons(
        self,
        df: DataFrame,
        entity: SilverEntityConfig,
    ) -> Column:
        """Compose a pipe-delimited rejection reason column."""
        checks: list[Column] = []

        for column in entity.required_columns:
            checks.append(
                F.when(F.col(column).isNull(), F.lit(f"null_{column}")).otherwise(
                    F.lit("")
                )
            )

        for column, allowed in entity.accepted_values.items():
            if column in df.columns:
                checks.append(
                    F.when(
                        F.col(column).isNotNull() & ~F.col(column).isin(allowed),
                        F.lit(f"invalid_{column}"),
                    ).otherwise(F.lit(""))
                )

        for column, constraint in entity.numeric_constraints.items():
            if column in df.columns:
                checks.append(self._numeric_violation(column, constraint))

        duplicate_flag = self._duplicate_key_flag(df, entity)
        checks.append(duplicate_flag)

        return self._concat_reasons(checks)

    def _numeric_violation(self, column: str, constraint: NumericConstraint) -> Column:
        """Build numeric constraint violation expression."""
        checks: list[Column] = []
        if constraint.min_inclusive is not None:
            checks.append(
                F.when(
                    F.col(column).isNotNull()
                    & (F.col(column) < F.lit(constraint.min_inclusive)),
                    F.lit(f"below_min_{column}"),
                ).otherwise(F.lit(""))
            )
        if constraint.min_exclusive is not None:
            checks.append(
                F.when(
                    F.col(column).isNotNull()
                    & (F.col(column) <= F.lit(constraint.min_exclusive)),
                    F.lit(f"below_min_exclusive_{column}"),
                ).otherwise(F.lit(""))
            )
        if constraint.max_inclusive is not None:
            checks.append(
                F.when(
                    F.col(column).isNotNull()
                    & (F.col(column) > F.lit(constraint.max_inclusive)),
                    F.lit(f"above_max_{column}"),
                ).otherwise(F.lit(""))
            )
        if constraint.max_exclusive is not None:
            checks.append(
                F.when(
                    F.col(column).isNotNull()
                    & (F.col(column) >= F.lit(constraint.max_exclusive)),
                    F.lit(f"above_max_exclusive_{column}"),
                ).otherwise(F.lit(""))
            )
        return self._concat_reasons(checks) if checks else F.lit("")

    def _duplicate_key_flag(self, df: DataFrame, entity: SilverEntityConfig) -> Column:
        """Flag duplicate primary keys within the batch."""
        window = Window.partitionBy(*entity.dedupe_keys)
        return F.when(
            F.count(F.lit(1)).over(window) > 1,
            F.lit(f"duplicate_{entity.primary_key}"),
        ).otherwise(F.lit(""))

    @staticmethod
    def _concat_reasons(checks: list[Column]) -> Column:
        """Concatenate non-empty reason fragments with pipe separator."""
        if not checks:
            return F.lit("")
        return reduce(
            lambda left, right: F.concat_ws(
                "|",
                F.nullif(left, F.lit("")),
                F.nullif(right, F.lit("")),
            ),
            checks,
        )

    @staticmethod
    def _has_failures(reasons: Column) -> Column:
        """Return True when any rejection reason is present."""
        return reasons.isNotNull() & (reasons != "")
