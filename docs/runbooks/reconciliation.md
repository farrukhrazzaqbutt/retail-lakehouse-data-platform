# Runbook — Reconciliation

## Purpose

`scripts/run_reconciliation.py` compares row counts across pipeline layers and writes a JSON report to `data/reconciliation/`.

## Run

```bash
python scripts/run_reconciliation.py
python scripts/run_reconciliation.py --skip-snowflake
```

Airflow runs this as the final task in `retail_daily_pipeline`.

## Report structure

```json
{
  "generated_at": "2026-07-11T18:00:00+00:00",
  "passed": false,
  "issues": ["customers: postgres=5000 bronze=4998"],
  "postgres": [{"entity": "customers", "row_count": 5000, "status": "ok"}],
  "bronze": [{"entity": "customers", "row_count": 4998, "status": "ok"}],
  "silver": [...],
  "gold": [...],
  "snowflake": [...]
}
```

## Interpreting issues

| Issue pattern | Meaning | Action |
|---------------|---------|--------|
| `postgres=N bronze=M` | Ingestion count drift | Re-run Phase 2 ingestion |
| `bronze count unavailable` | No landing data | Run `run_local_ingestion.py` |
| `postgres: error` | DB connection failed | Check Docker Postgres |
| `silver/gold: missing` | Manifest not found | Run Silver/Gold pipeline |
| Snowflake mismatch | Load incomplete | Run `validate_snowflake.py` |

## Manifest-based layers

Silver, Gold, and Snowflake counts come from the latest `*_run_*.json` manifest — not live queries. If manifests are stale, re-run the corresponding pipeline.

## Exit codes

- `0` — all checks passed
- `1` — one or more issues found
