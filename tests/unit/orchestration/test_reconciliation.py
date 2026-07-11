"""Tests for reconciliation reporting."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from retail_lakehouse.orchestration.reconciliation import (
    LayerCount,
    ReconciliationReporter,
)


def test_reconciliation_result_serialization() -> None:
    reporter = ReconciliationReporter()
    result = reporter.run(skip_snowflake=True)
    payload = result.to_dict()
    assert "generated_at" in payload
    assert "postgres" in payload
    assert "issues" in payload


def test_write_report(tmp_path: Path) -> None:
    reporter = ReconciliationReporter(output_dir=tmp_path)
    result = reporter.run(skip_snowflake=True)
    path = reporter.write_report(result)
    assert path.exists()
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["passed"] == result.passed


def test_compare_layers_detects_mismatch() -> None:
    reporter = ReconciliationReporter()
    result = reporter.run(skip_snowflake=True)
    result.postgres_counts = [
        LayerCount(entity="customers", row_count=100, status="ok")
    ]
    result.bronze_counts = [LayerCount(entity="customers", row_count=90, status="ok")]
    reporter._compare_layers(result)
    assert any("customers" in issue for issue in result.issues)


def test_postgres_counts_handles_connection_error() -> None:
    reporter = ReconciliationReporter()
    with patch.object(
        reporter,
        "postgres_config",
        MagicMock(sqlalchemy_url="postgresql://invalid"),
    ):
        with patch(
            "retail_lakehouse.orchestration.reconciliation.create_engine",
            side_effect=Exception("connection failed"),
        ):
            counts = reporter._postgres_counts()
    assert counts[0].status == "error"
