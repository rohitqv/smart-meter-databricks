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

Prereqs: `uv`, `terraform >= 1.6`, `aws` CLI, `databricks` CLI, `jq`. Auth: AWS creds, `DATABRICKS_HOST`, `DATABRICKS_TOKEN`. Copy `terraform/terraform.tfvars.example` → `terraform/terraform.tfvars` and set `databricks_host`.

### Pipeline lifecycle

The pipeline has two phases — **backfill** (historical + NRT) and **steady state** (NRT only) — connected by a one-time phase transition. Follow these steps in order:

| # | Step | Command | What happens |
|---|------|---------|--------------|
| 0 | Setup | `make install && make tf-init` | Create venv, install deps, run unit tests, initialize Terraform |
| 1 | Seed S3 | `make seed` | Generate historical files + 1 NRT bootstrap tick into S3 |
| 2 | Backfill deploy | `make deploy-backfill` | `terraform apply -var merge_historical=true` — creates catalog, schemas, DLT pipeline |
| 3 | Backfill run | `make pipeline-run` | `--full-refresh` builds bronze/silver/gold from NRT ∪ historical (2-source streaming plan) |
| 4 | Verify | `make verify` | `SHOW TABLES` across `bronze`/`silver`/`gold` — confirms all tables populated |
| 5 | Steady-state deploy | `make deploy` | `terraform apply` flips `merge_historical` to `false` (metadata-only, no pipeline run) |
| 6 | **Phase-transition refresh** | `make pipeline-run` | ⚠️ **Required.** `--full-refresh` rewrites streaming checkpoints from 2-source to 1-source shape |
| 7 | Steady state | _(NRT cron, every 15 min)_ | NRT simulator writes to S3 → Auto Loader picks up → silver/gold update incrementally |

> **⚠️ Don't skip step 6.** After flipping `merge_historical` to `false` (step 5), the existing streaming checkpoints still expect 2 sources. Without a full refresh the pipeline fails with: `assertion failed: There are [2] sources in the checkpoint offsets and now there are [1] sources requested by the query. Cannot continue.` See [`docs/databricks-learnings.md` Part 8.5–8.6](docs/databricks-learnings.md) for the full explanation.

`make help` lists all targets. Override defaults inline: `make seed BUCKET=other-bucket`.

### Operational notes

- **NRT simulator:** the cron job is created **paused** (step 2). Un-pause via the Databricks Workflows UI after step 6 succeeds, once you're confident bronze NRT tables are picking up files.
- **Quarantine routing:** failed DQX rows land in `silver.<table>_quarantine` (and `gold.<kpi>_quarantine`) with `_quarantined_at` and DQX failure metadata.
- **Deeper context:** see [`docs/databricks-learnings.md`](docs/databricks-learnings.md) for detailed explanations of DLT, Unity Catalog, Auto Loader, the merge_historical design, and common errors.

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
