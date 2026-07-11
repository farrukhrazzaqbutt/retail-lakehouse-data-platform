"""Tests for reconciliation manifest parsing."""

from __future__ import annotations

import json
from pathlib import Path

from retail_lakehouse.orchestration.reconciliation import ReconciliationReporter


def test_manifest_counts_reads_latest_manifest(tmp_path: Path) -> None:
    manifest_dir = tmp_path / "manifests"
    manifest_dir.mkdir()
    older = manifest_dir / "gold_run_old.json"
    newer = manifest_dir / "gold_run_new.json"
    older.write_text(
        json.dumps({"tables": [{"table_name": "dim_date", "row_count": 1}]}),
        encoding="utf-8",
    )
    newer.write_text(
        json.dumps(
            {
                "tables": [
                    {"table_name": "dim_date", "row_count": 10},
                    {"table_name": "dim_customers", "row_count": 5},
                ]
            }
        ),
        encoding="utf-8",
    )
    older.touch()
    newer.touch()
    import os
    import time

    time.sleep(0.01)
    os.utime(newer, (newer.stat().st_mtime + 10, newer.stat().st_mtime + 10))

    reporter = ReconciliationReporter()
    counts = reporter._manifest_counts(manifest_dir, "gold", prefix="gold_run_")
    entities = {layer.entity: layer.row_count for layer in counts}
    assert entities["dim_date"] == 10
    assert entities["dim_customers"] == 5
