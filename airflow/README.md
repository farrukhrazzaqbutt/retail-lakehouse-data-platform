# Airflow Orchestration

Apache Airflow DAGs for the Retail Lakehouse Data Platform (Phase 8).

## DAGs

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `retail_platform_setup` | Manual | Snowflake + dbt schema setup |
| `retail_daily_pipeline` | `0 2 * * *` | Full Phases 1→7 + reconciliation |
| `retail_health_check` | `0 6 * * 1` | Weekly dry-run and compile checks |

## Quick Start

```bash
# Generate Fernet key (once)
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Add to .env as AIRFLOW__CORE__FERNET_KEY

docker compose --profile airflow up -d
# UI: http://localhost:8080 (admin / admin by default)
```

See [docs/phase8-airflow-orchestration.md](../docs/phase8-airflow-orchestration.md).
