# Contributing

Thank you for contributing to the Retail Lakehouse Data Platform.

## Development setup

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\Activate.ps1
pip install -r requirements.txt -r requirements-dev.txt -r requirements-dbt.txt
cp .env.example .env
```

Optional:

```bash
pip install -r requirements-airflow.txt
pre-commit install
```

## Quality gates

Run the full local CI pipeline:

```bash
python scripts/run_ci_local.py
```

Or step by step:

| Command | Purpose |
|---------|---------|
| `ruff check src tests scripts` | Lint |
| `ruff format --check src tests scripts` | Format check |
| `mypy src/retail_lakehouse` | Type check |
| `pytest -m "not spark" -q` | Unit tests (no Java) |
| `pytest -m spark -q` | Spark tests (requires Java 17+) |
| `pytest --cov=retail_lakehouse --cov-report=term-missing` | Coverage |

Makefile equivalents: `make ci-local`, `make lint`, `make coverage`

## Pull requests

1. Branch from `main`
2. Keep changes focused and config-driven
3. Add or update tests for new behavior
4. Update relevant phase docs when changing pipeline behavior
5. Never commit `.env`, generated data, or secrets
6. Use the PR template checklist

## Test guidelines

- **Unit tests** — fast, no external services (`tests/unit/`)
- **Spark tests** — auto-marked; require Java (`tests/unit/transforms`, `gold`, `warehouse`)
- **Integration tests** — config/load smoke tests (`tests/integration/`)
- **Script smoke** — every `scripts/*.py` must support `--help`

## Documentation

- Phase docs: `docs/phase*.md`
- Runbooks: `docs/runbooks/`
- Architecture: `docs/architecture.md`

## Code style

- Python 3.11+ with type hints
- Line length 88 (Ruff)
- Match existing module patterns in `src/retail_lakehouse/`
- Click CLIs for scripts with `--log-level`

## Questions

Open an issue or discussion on GitHub for design questions before large refactors.
