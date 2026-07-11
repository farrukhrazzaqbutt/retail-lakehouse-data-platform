#!/usr/bin/env python3
"""Validate dbt project compile and tests."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import click

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt"


def _run_dbt(command: list[str], env: dict[str, str]) -> int:
    """Execute a dbt CLI command in the project directory."""
    result = subprocess.run(
        command,
        cwd=DBT_PROJECT_DIR,
        env=env,
        check=False,
    )
    return result.returncode


def _dbt_env() -> dict[str, str]:
    """Build environment with dbt profile directory."""
    env = os.environ.copy()
    env.setdefault("DBT_PROFILES_DIR", str(DBT_PROJECT_DIR / "profiles"))
    env.setdefault("DBT_TARGET", "dev")
    return env


@click.command()
@click.option(
    "--compile-only",
    is_flag=True,
    help="Only run dbt compile (no Snowflake connection required for parse)",
)
@click.option(
    "--skip-tests",
    is_flag=True,
    help="Skip dbt test after compile",
)
@click.option(
    "--log-level", default="INFO", help="Logging level (unused, for CLI parity)"
)
def main(compile_only: bool, skip_tests: bool, log_level: str) -> None:
    """
    Validate dbt project by compiling and running tests.

    Example:
        python scripts/validate_dbt.py --compile-only
        python scripts/validate_dbt.py
    """
    del log_level
    env = _dbt_env()

    click.echo("Installing dbt packages (dbt deps)...")
    code = _run_dbt(["dbt", "deps"], env)
    if code != 0:
        raise SystemExit(code)

    click.echo("Running dbt compile...")
    code = _run_dbt(["dbt", "compile"], env)
    if code != 0:
        raise SystemExit(code)
    click.echo("  [PASS] dbt compile")

    if compile_only:
        click.echo("\nCompile-only validation passed.")
        return

    if skip_tests:
        click.echo("\nSkipped dbt test.")
        return

    click.echo("Running dbt test...")
    code = _run_dbt(["dbt", "test"], env)
    if code != 0:
        raise SystemExit(code)

    click.echo("\nAll dbt validation checks passed.")


if __name__ == "__main__":
    main()
