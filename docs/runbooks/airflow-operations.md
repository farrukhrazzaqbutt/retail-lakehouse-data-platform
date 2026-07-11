# Runbook — Airflow Operations

## Services

```bash
docker compose --profile airflow up -d
docker compose ps
```

| Service | URL / Role |
|---------|------------|
| Webserver | http://localhost:8080 |
| Scheduler | Background DAG execution |
| Postgres | Metadata DB `airflow_db` |

Default login: `admin` / `admin` (override via `.env`).

## DAGs

| DAG | When to use |
|-----|-------------|
| `retail_platform_setup` | Once after deploy — Snowflake + dbt schemas |
| `retail_daily_pipeline` | Daily full ETL (02:00 UTC) |
| `retail_health_check` | Weekly dry-run checks |

## Trigger a DAG manually

1. Open Airflow UI → DAGs
2. Unpause the DAG
3. Click **Trigger DAG**

## View logs

1. DAG → Grid view → click task square
2. **Log** button for stdout/stderr

## Common failures

| Task | Likely cause | Fix |
|------|--------------|-----|
| `wait_for_postgres` | Postgres not healthy | `docker compose up -d postgres` |
| `run_silver_transforms` | No Java in container | Rebuild `airflow/Dockerfile` image |
| `run_snowflake_load` | Missing credentials | Set `SNOWFLAKE_*` in `.env` |
| `run_dbt_models` | Schema missing | Run `retail_platform_setup` first |
| `run_reconciliation` | Bronze/Postgres mismatch | Re-run Phase 1–2 |

## Restart Airflow

```bash
docker compose --profile airflow restart airflow-scheduler airflow-webserver
```

## Re-init metadata DB

```bash
docker compose --profile airflow run --rm airflow-init
```

Only when metadata is corrupted or on first install.
