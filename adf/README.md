# Azure Data Factory — Phase 2 Ingestion

This folder contains **Azure Data Factory** artifacts for ingesting retail source data into **Azure Data Lake Storage Gen2**.

## Architecture

```
PostgreSQL (5 tables)  ──► PL_Ingest_PostgreSQL_Table ──► ADLS raw/bronze/postgres/
Product Updates (CSV)  ──► PL_Ingest_File_Source      ──► ADLS raw/bronze/file/
Website Events (JSON)  ──► PL_Ingest_File_Source      ──► ADLS raw/bronze/file/
                              ▲
                    PL_Master_Ingestion (orchestrator)
                              ▲
                    TR_Daily_Ingestion (schedule)
```

## Folder Structure

| Path | Description |
|------|-------------|
| `linkedService/` | PostgreSQL, ADLS Gen2, Key Vault connections |
| `dataset/` | Parameterized source and sink datasets |
| `pipeline/` | Table copy, file copy, and master orchestration |
| `trigger/` | Daily schedule trigger |
| `arm/` | ARM deployment parameters template |

## Landing Zone Layout

All sinks use partitioned paths with ingestion metadata columns:

```
raw/
└── bronze/
    ├── postgres/
    │   ├── customers/ingestion_date=YYYY-MM-DD/batch_id=<id>/customers.parquet
    │   ├── products/...
    │   ├── orders/...
    │   ├── order_items/...
    │   └── payments/...
    └── file/
        ├── product_updates/ingestion_date=YYYY-MM-DD/batch_id=<id>/
        └── website_events/ingestion_date=YYYY-MM-DD/batch_id=<id>/
```

**Metadata columns added at ingest time:**

- `batch_id`
- `source_system`
- `source_file`
- `ingested_at`
- `ingestion_date`

## Deployment Options

### Option A — Azure Portal (Import)

1. Create an Azure Data Factory instance.
2. Open **Author** → **+** → **From JSON**.
3. Import each file in order:
   - Linked services (`linkedService/`)
   - Datasets (`dataset/`)
   - Pipelines (`pipeline/`)
   - Trigger (`trigger/`)
4. Configure Key Vault secret `postgres-password`.
5. Publish and trigger `PL_Master_Ingestion`.

### Option B — Azure CLI

```bash
# Set variables
RESOURCE_GROUP="rg-retail-lakehouse"
FACTORY_NAME="adf-retail-lakehouse-dev"
SUBSCRIPTION_ID="<your-subscription-id>"

az account set --subscription $SUBSCRIPTION_ID

# Deploy linked services, datasets, pipelines via ADF Git publish
# or use az datafactory linked-service create with JSON bodies from this folder
```

### Option C — Local Development (No Azure Required)

Use the Python mirror of ADF copy semantics:

```bash
python scripts/generate_file_sources.py --products 50 --customers 100
python scripts/run_local_ingestion.py
python scripts/validate_phase2.py
```

## Pipeline Parameters

`PL_Master_Ingestion` accepts:

| Parameter | Description |
|-----------|-------------|
| `batchId` | Unique batch identifier (auto-generated via `guid()`) |
| `ingestionDate` | Partition date (`yyyy-MM-dd`) |
| `storageAccountUrl` | ADLS Gen2 endpoint |
| `filesystem` | ADLS filesystem (default: `raw`) |
| `postgresHost` | PostgreSQL hostname |
| `postgresPort` | PostgreSQL port (default: `55432`) |
| `postgresDatabase` | Database name |
| `postgresUser` | Database user |

## Security

- PostgreSQL password stored in **Azure Key Vault** (`LS_AzureKeyVault`).
- Storage access via managed identity or service principal (configure in Azure portal).
- No secrets committed to this repository — see `.env.example`.

## Interview Notes

1. **Why ADF for ingestion?** Managed connectors, monitoring, and parameterized pipelines without custom orchestration code.
2. **Why partition by `ingestion_date`?** Enables incremental Bronze processing and time-travel debugging in Phase 3.
3. **Why metadata columns?** Supports lineage, reconciliation, and idempotent reprocessing in Databricks Silver/Gold layers.
