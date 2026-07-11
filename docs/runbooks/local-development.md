# Runbook — Local Development

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.11+ | Pipeline scripts and tests |
| Docker | Latest | PostgreSQL, Airflow |
| Java | 17+ | PySpark Silver/Gold/Snowflake load |
| Git | Latest | Version control |

Optional: Snowflake account, Azure credentials for production paths.

## First-time setup

```bash
git clone <repo-url> retail-lakehouse-data-platform
cd retail-lakehouse-data-platform
python -m venv .venv
.venv\Scripts\Activate.ps1          # Windows
pip install -r requirements.txt -r requirements-dev.txt
cp .env.example .env
docker compose up -d
```

## Java / Spark

Verify Java:

```bash
java -version
```

Set `JAVA_HOME` if needed. Spark tests skip automatically when Java is missing.

## Run tests

```bash
pytest -m "not spark" -q
pytest -m spark -q
python scripts/run_ci_local.py
```

## Start Airflow

```bash
docker compose --profile airflow up -d
```

UI: http://localhost:8080

## Common issues

| Issue | Fix |
|-------|-----|
| Postgres port conflict | Set `POSTGRES_PORT=55432` in `.env` |
| `POSTGRES_PASSWORD` not set | Copy `.env.example` → `.env` |
| Spark tests skipped | Install Java 17+ |
| Airflow Fernet key error | Generate key and set `AIRFLOW__CORE__FERNET_KEY` |
| Snowflake tasks fail | Configure `SNOWFLAKE_*` in `.env` or use `--dry-run` |
