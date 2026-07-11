#!/usr/bin/env python3
"""Generate end-to-end pipeline reconciliation report."""

from __future__ import annotations

import sys
from pathlib import Path

import click

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from retail_lakehouse.orchestration.reconciliation import (  # noqa: E402
    ReconciliationReporter,
)
from retail_lakehouse.utils.logging import configure_logging, get_logger  # noqa: E402

logger = get_logger(__name__)


@click.command()
@click.option(
    "--output-dir",
    default=None,
    type=click.Path(path_type=Path),
    help="Directory for reconciliation JSON reports",
)
@click.option(
    "--skip-snowflake",
    is_flag=True,
    help="Skip Snowflake manifest checks",
)
@click.option("--log-level", default="INFO", help="Logging level")
def main(
    output_dir: Path | None,
    skip_snowflake: bool,
    log_level: str,
) -> None:
    """
    Compare row counts across Postgres, Bronze, and lakehouse manifests.

    Example:
        python scripts/run_reconciliation.py
        python scripts/run_reconciliation.py --skip-snowflake
    """
    configure_logging(log_level)
    reporter = ReconciliationReporter(output_dir=output_dir)
    result = reporter.run(skip_snowflake=skip_snowflake)
    report_path = reporter.write_report(result)

    click.echo(f"Reconciliation report: {report_path}")
    click.echo(f"Status: {'PASSED' if result.passed else 'FAILED'}")
    click.echo(f"Issues: {len(result.issues)}")

    if result.postgres_counts:
        click.echo("\nPostgreSQL counts:")
        for layer in result.postgres_counts:
            click.echo(f"  {layer.entity}: {layer.row_count} ({layer.status})")

    if result.bronze_counts:
        click.echo("\nBronze counts:")
        for layer in result.bronze_counts:
            click.echo(f"  {layer.entity}: {layer.row_count} ({layer.status})")

    if result.issues:
        click.echo("\nIssues:")
        for issue in result.issues:
            click.echo(f"  - {issue}")
        raise SystemExit(1)

    click.echo("\nReconciliation passed.")


if __name__ == "__main__":
    main()
