#!/usr/bin/env python3
"""Run local CI checks (lint, typecheck, tests, coverage)."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import click

PROJECT_ROOT = Path(__file__).resolve().parents[1]
HAS_JAVA = bool(shutil.which("java") or __import__("os").getenv("JAVA_HOME"))


def _run(command: list[str], label: str) -> int:
    """Run a command and print its label."""
    print(f"\n=== {label} ===")
    result = subprocess.run(command, cwd=PROJECT_ROOT, check=False)
    return result.returncode


def _run_ci() -> None:
    """Execute the local CI pipeline."""
    steps = [
        (["ruff", "check", "src", "tests", "scripts"], "Ruff lint"),
        (
            ["ruff", "format", "--check", "src", "tests", "scripts"],
            "Ruff format",
        ),
        (["mypy", "src/retail_lakehouse"], "Mypy"),
        (["pytest", "-m", "not spark", "-q"], "Pytest (non-Spark)"),
    ]
    if HAS_JAVA:
        steps.append((["pytest", "-m", "spark", "-q"], "Pytest (Spark)"))
    steps.append(
        (
            [
                "pytest",
                "--cov=retail_lakehouse",
                "--cov-report=term-missing",
                "-q",
            ],
            "Coverage",
        )
    )

    failures: list[str] = []
    for command, label in steps:
        code = _run(command, label)
        if code != 0:
            failures.append(label)

    if failures:
        print("\nLocal CI failed:")
        for label in failures:
            print(f"  - {label}")
        raise SystemExit(1)

    print("\nAll local CI checks passed.")


@click.command()
def main() -> None:
    """Run lint, typecheck, tests, and coverage checks."""
    _run_ci()


if __name__ == "__main__":
    main()
