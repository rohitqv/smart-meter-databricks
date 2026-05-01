# smart-meter-databricks

Smart-grid meter telemetry pipeline on **Databricks Free Edition**, demonstrating
medallion architecture (Bronze → Silver → Gold) with two ingestion lanes
(historical bulk + NRT simulator), DQX-driven data quality, and Terraform-managed
workspace resources.

## Layout

| Path | Purpose |
|------|---------|
| `terraform/` | Workspace IaC (catalog, schemas, DLT pipeline, jobs, file uploads) |
| `notebooks/` | DLT pipeline source: bronze, silver, gold |
| `ingestion/` | Pure-Python data generators (historical + NRT) |
| `data_quality/` | DQX rule YAMLs per silver/gold table |
| `tests/` | Unit + integration tests |
| `docs/` | Requirements notebook and design docs |

## Quickstart

See `docs/superpowers/plans/2026-05-01-smart-grid-meter.md` for the full implementation walk-through.

Steady-state runtime config: `merge_historical = False` (NRT-only).
For initial backfill: temporarily set `merge_historical = True`, run pipeline once, flip back.

## S3 Layout

```
s3://bkt-ry-smart-grid-meter-bucket/
├── raw/historical/<table>/<table>_historical_<NNNN>.json   # fixed paths
├── raw/nrt/<table>/dt=YYYY-MM-DD/<table>_<UTC-ts>_<NNNN>.json   # timestamped
├── _checkpoints/<bronze_table>/                            # Auto Loader state
└── _schema/<bronze_table>/                                 # Auto Loader schema
```

## Constraints

Databricks Free Edition limits — see `docs/superpowers/specs/2026-05-01-smart-grid-meter-design.md` § 2.
