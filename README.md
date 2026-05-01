# smart-meter-databricks

Smart-grid meter telemetry pipeline on **Databricks Free Edition**, demonstrating
medallion architecture (Bronze → Silver → Gold) with two ingestion lanes
(historical bulk + NRT simulator), DQX-driven data quality, and Terraform-managed
workspace resources.

## Layout

| Path | Purpose |
|------|---------|
| `terraform/` | Workspace IaC (catalog, schemas, DLT pipeline, jobs, file uploads) |
| `notebooks/` | DLT pipeline source: `01_bronze_ingest.py`, `02_silver_transform.py`, `03_gold_kpis.py` |
| `ingestion/` | Pure-Python data generators: `_common.py` (schemas/Faker), `historical/`, `nrt/` |
| `data_quality/` | DQX rule YAMLs per silver/gold table |
| `tests/unit/` | pytest unit tests for ingestion code |
| `tests/integration/` | live-workspace smoke test (gated on `DATABRICKS_TOKEN`) |
| `docs/` | Requirements notebook |

## Quickstart

```bash
# 1. Install dev deps
uv venv --python 3.11
source .venv/bin/activate
uv pip install -e ".[dev]"

# 2. Run unit tests
pytest tests/unit/ -v

# 3. Generate historical data into S3
python -m ingestion.historical.load_historical --bucket bkt-ry-smart-grid-meter-bucket

# 4. Deploy workspace resources (first run: merge_historical=true to backfill from S3)
cd terraform
terraform init
terraform apply -auto-approve -var merge_historical=true
cd ..

# 5. Trigger the DLT pipeline
databricks pipelines start-update <pipeline-id> --full-refresh

# 5b. After backfill completes, flip back to NRT-only steady state
cd terraform
terraform apply -auto-approve   # default: merge_historical=false
cd ..

# 6. Smoke test
DATABRICKS_TOKEN=... pytest tests/integration/test_smoke.py -v
```

## Operational notes

- **Initial backfill:** pass `-var merge_historical=true` to `terraform apply` for the first pipeline run, then re-apply with the default (`false`) to switch to NRT-only steady state.
- **NRT simulator:** the cron job is created paused. Un-pause via the Databricks Workflows UI once you're confident the bronze NRT tables are picking up files.
- **Quarantine routing:** failed DQX rows land in `silver.<table>_quarantine` (and `gold.<kpi>_quarantine`) with `_quarantined_at` and DQX failure metadata.

## S3 Layout

```
s3://bkt-ry-smart-grid-meter-bucket/
├── raw/historical/<table>/<table>_historical_<NNNN>.json    # fixed paths
├── raw/nrt/<table>/dt=YYYY-MM-DD/<table>_<UTC-ts>_<NNNN>.json   # timestamped
├── _checkpoints/<bronze_table>/                              # Auto Loader state
└── _schema/<bronze_table>/                                   # Auto Loader schema
```

## Constraints

Databricks Free Edition hard limits shape the design:

- Serverless-only compute (no classic clusters or job compute)
- One DLT pipeline per type (single pipeline runs bronze, silver, gold)
- S3 external locations supported via Unity Catalog
- No account-level APIs (workspace-scoped operations only)
- Single workspace deployment
