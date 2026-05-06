# Databricks Learnings — Smart Grid Meter Pipeline

## Audience

You know ETL, data warehousing, SQL, and PySpark. You're newer to DevOps,
Terraform, and the Databricks-specific abstractions. This document maps your
existing knowledge onto Databricks: Unity Catalog, Lakeflow Declarative
Pipelines (DLT), Auto Loader, DQX, and the Terraform deploy model. Examples
come from the smart-meter project we built.

It's organized roughly in the order things became relevant during the build.

---

## Part 1: Mental model — where your code actually runs

### 1.1 Two destinations, one runtime

Three places hold your code:

- **Your laptop** — where you edit
- **GitHub** — source of truth, history, PRs. *Pushing here doesn't deploy anything.*
- **Databricks workspace** — the actual runtime. Code lives at workspace paths
  like `/Workspace/Shared/dev_smart_grid/notebooks/02_silver_transform`.

In this project, **Terraform uploads your local files to the Databricks
workspace via REST API**. `terraform apply` reads `notebooks/02_silver_transform.py`
from disk and writes it to the workspace path. The DLT pipeline references
the workspace path, not the GitHub URL.

```
local files ──[terraform apply]──▶ Databricks workspace ──[pipeline run]──▶ DLT runtime
```

GitHub is parallel to this loop, not part of it.

The alternative — **Databricks Repos** (renamed *Git folders* in the UI),
which clones a git repo directly into the workspace — is covered in 1.4. Most
teams pick one or the other; mixing them gets confusing.

### 1.2 Control plane vs compute plane

Databricks separates:

- **Control plane** — the UI, schedulers, metastore (hosted by Databricks)
- **Compute plane** — the VMs that run your Spark code

Two ways to get compute:
- **Classic compute** — clusters you size and configure
- **Serverless compute** — Databricks manages the VMs; you submit work

This project uses serverless because Free Edition only allows serverless. You
don't pick a cluster size; the pipeline scales itself.

### 1.3 What "pipeline" means here

In this repo "pipeline" specifically means a **Lakeflow Declarative Pipeline
(DLT)**. DLT is a managed runtime that:

1. Reads your notebooks
2. Builds a DAG from `@dlt.table` and `@dlt.view` definitions
3. Resolves dependencies and runs them in order
4. Tracks state (Delta checkpoints, Auto Loader schema location, watermarks)
5. Manages retries and incremental materialization

Mental shortcut: **dbt + Spark Structured Streaming + Airflow, fused into one
product**. You declare what you want; DLT figures out how to build it.

### 1.4 The alternative pattern — Databricks Repos (Git folders)

Now that you've seen what gets deployed in this project (notebooks plus the
catalog, schemas, pipeline definition, and cron job), the comparison with the
Repos pattern is concrete enough to be useful.

Databricks Repos — renamed **Git folders** in the UI — is the other way to
get code into the workspace. Instead of Terraform reading local files and
uploading them, you register a remote git repo with Databricks; the workspace
then holds a real git clone pinned to a commit. The pipeline references files
at `/Workspace/Repos/<owner>/<repo>/notebooks/...` instead of
`/Workspace/Shared/...`.

The two patterns differ on **who pushes the code, and when**.

```
github main ──[GH Action: repos update]──▶ workspace clone ──[pipeline run]──▶ DLT runtime
```

#### What triggers the pull?

There is no automatic sync. A Git folder does not poll GitHub, and GitHub does
not push to it. The folder stays on whatever commit it was last updated to
until something explicitly tells Databricks to fast-forward. That something is
one API call:

```
PATCH /api/2.0/repos/{id}    # CLI: databricks repos update <path> --branch main
```

That call is the equivalent of `git fetch && git checkout main` inside the
workspace folder, against the remote it was originally cloned from. Production
teams pick one of two triggers for it:

1. **GitHub Action on `push: main`.** The merged PR fires a workflow that
   calls `databricks repos update`. Auth via OIDC or a service-principal
   token. Latency: seconds. This is the common pattern.

   ```yaml
   on:
     push:
       branches: [main]
   jobs:
     sync:
       runs-on: ubuntu-latest
       steps:
         - uses: databricks/setup-cli@main
         - run: databricks repos update /Workspace/Repos/prod/smart-grid --branch main
           env:
             DATABRICKS_HOST:  ${{ vars.DATABRICKS_HOST }}
             DATABRICKS_TOKEN: ${{ secrets.SP_TOKEN }}
   ```

2. **Scheduled Databricks job.** A small notebook on a cron that calls the
   Repos API. Used when external CI can't (or shouldn't) hold credentials into
   Databricks. Trades latency for simplicity.

The merge to `main` is *not* the trigger. The thing listening for the merge —
the Action or the cron — is.

#### Post-merge lifecycle, side by side

| | Git folders pattern | This project (Terraform) |
|---|---|---|
| Engineer loop | branch → PR → review → merge to `main` | same |
| Sync trigger | GH Action fires on `push: main` | `terraform apply` (laptop or CI) |
| What runs | `databricks repos update --branch main` | Provider reads local files, `POST /api/2.0/workspace/import` per file |
| Result in workspace | Folder fast-forwards to new HEAD | Notebook contents overwritten in place |
| Branch awareness | Yes — folder is a real clone; supports `git checkout` in the UI | No — just files; workspace doesn't know they came from git |
| Rollback | API update to an earlier SHA | Re-apply an older Terraform revision |
| What else gets deployed | Code only | Catalog, schemas, external locations, pipeline definition, jobs |

Both designs end the same way: `/Workspace/...` holds the latest code, and the
next pipeline run picks it up — pipeline *triggering* is independent of code
sync in either pattern (Part 10.5).

#### Why this project went Terraform instead

Git folders sync **code only**. They don't create the catalog, the schemas,
the external locations, the pipeline definition, or the NRT cron job. For
this project — where every one of those is part of the deploy — we'd still
need Terraform (or Databricks Asset Bundles) for everything except the
notebook files. Splitting "notebooks via Repos, infra via IaC" is the mixing
1.1 warns against: two tools competing for the same workspace paths, two
sources of truth, two ways to roll back.

The deciding question is roughly: *does the deploy include anything beyond
notebooks?* If yes, an IaC tool already owns the workspace — let it own the
notebooks too. If no (a notebook-heavy ML workflow on pre-existing infra, for
example), Repos is the lighter pick.

---

## Part 2: Unity Catalog (data organization)

Databricks's modern metastore. Replaces the older Hive metastore.

### 2.1 The three-level namespace

Every table has a three-part name: `catalog.schema.table`.

In this project:

| Level | Value |
|---|---|
| Catalog | `dev_smart_grid` |
| Schemas | `bronze`, `silver`, `gold` |
| Example | `dev_smart_grid.silver.fact_readings` |

Coming from a traditional warehouse: catalog ≈ database, schema ≈ schema. The
third level is new for Spark users used to two-part names.

### 2.2 Storage roots

A catalog needs to know where to put Delta files. From `terraform/catalog.tf`:

```hcl
resource "databricks_catalog" "smart_grid" {
  name         = local.catalog_name
  storage_root = "${var.bucket_url}/_uc_managed/${local.catalog_name}"
}
```

Tables created in this catalog land under that S3 prefix. Free Edition's
default storage isn't auto-discovered by the Terraform provider, so we pass
an explicit root. In real environments, this is also useful — you want to
know exactly where your data physically sits.

### 2.3 Why fully-qualified names matter

DLT in **Direct Publishing Mode (DPM)** requires `catalog.schema.table` for
cross-schema reads. From `notebooks/01_bronze_ingest.py`:

```python
@dlt.table(
    name=f"{CATALOG}.bronze.{bronze_name}",  # full 3-part name
    ...
)
```

We initially used unqualified names like `name=bronze_name`. The bronze
notebook itself worked (its default schema is `bronze`), but the moment silver
tried `dev_smart_grid.bronze.X`, DLT replied:

> Dataset is defined in the pipeline but could not be resolved.

**Rule**: always use 3-part names for `@dlt.table` declarations and any
`dlt.read*` that crosses schemas. Saves a deploy round-trip.

---

## Part 3: Lakeflow Declarative Pipelines (DLT)

The heart of this project. If you've used dbt, the model is similar.

### 3.1 Three things you can declare

| Type | Decorator | Materialized? | Streaming? |
|---|---|---|---|
| Streaming table | `@dlt.table` returning `dlt.read_stream(...)` | yes (Delta) | yes |
| Materialized view | `@dlt.table` returning `dlt.read(...)` | yes (Delta) | no |
| View | `@dlt.view` | no — re-evaluated on read | depends on source |

A **streaming table** uses Spark Structured Streaming under the hood, with
checkpoints in Delta. New files in S3 → new rows appended.

A **materialized view** (created with `@dlt.table` but reading via `dlt.read`)
is recomputed each pipeline run.

A **view** is logical — no storage, just a query saved with a name. Used
heavily in this project for intermediate transforms (`int_*_vw`).

### 3.2 `dlt.read` vs `dlt.read_stream` — the most important distinction

This caused most of our pipeline failures. Memorize it:

- `dlt.read("name")` — **batch read**. Returns a snapshot DataFrame.
  Allowed on: batch views, streaming tables (read as snapshot), materialized views.
- `dlt.read_stream("name")` — **streaming read**. Returns a streaming DataFrame.
  Allowed on: streaming views, streaming tables.

DLT enforces a compatibility check:

| Source view type | Consumer must use |
|---|---|
| Streaming view (any function calls `dlt.read_stream` internally) | `dlt.read_stream` |
| Batch view (only calls `dlt.read` internally) | `dlt.read` |

Mismatched calls produce:
- `View 'X' is a streaming view and must be referenced using readStream`
- `View 'X' is not a streaming view and must be referenced using read`

You *can* disable the check with `pipelines.incompatibleViewCheck.enabled =
false`, but **don't**. Batch operations assume a finite, complete dataset; a
stream is neither. Three concrete failures the check is protecting you from:

- `count()` on a streaming view returns "however many rows had arrived by the
  moment this micro-batch ran" — a different answer every trigger, with no
  notion of *the* answer. Looks like a number; isn't one.
- `dropDuplicates()` without a watermark has to remember every row ever seen
  to know what's a duplicate. The state store grows without bound and the
  job eventually OOMs.
- `orderBy()` on an unbounded source has no defined endpoint to sort against,
  so the result reflects only what's currently buffered.

Spark allows these operations syntactically; the results are silently wrong
rather than failing loudly. The compatibility check is the engine catching
the mistake at plan time instead of letting it ship to production.

### 3.3 Why we ended up with two versions of the same view

In `notebooks/02_silver_transform.py`:

```python
# Streaming version — feeds SCD writes (apply_changes needs streaming sources)
@dlt.view(name="int_weather_station")
def int_weather_station():
    df = _unioned_bronze("weather_station")  # internally dlt.read_stream
    return _add_sequence_struct(...)

# Batch version — feeds gold KPIs and int_dim_geography
@dlt.view(name="int_weather_station_batch")
def int_weather_station_batch():
    return _unioned_bronze_batch("weather_station").withColumn(  # internally dlt.read
        "recorded_ts", to_timestamp(col("recorded_at"))
    )
```

The streaming version feeds `apply_changes` (which requires a streaming source).
The batch version feeds gold KPIs that need `dropDuplicates()` and `crossJoin()`
— operations illegal on streams without watermarks.

**Rejected alternative**: disable the compatibility check.
**Rejected alternative**: add watermarks to enable streaming dropDuplicates.
**Adopted**: define both view variants, route consumers explicitly.

Slight duplication; clearer semantics; cheap to maintain.

### 3.4 The DAG and how to read failures

DLT walks all your notebooks at pipeline start, builds a graph from `dlt.read*`
calls, and runs nodes in topological order. If one node fails, every downstream
node reports:

> Dataset 'X' is defined in the pipeline but could not be resolved

**This is a cascade message, not the root cause.** When debugging, sort the
event log by time **ascending** and find the first error. We hit cascades
repeatedly — the chain of "could not be resolved" errors at the top of the log
points to the actual broken node further down (in time) or further upstream
(in the DAG).

---

## Part 4: The medallion architecture in practice

Bronze → silver → gold. The project follows it strictly:

### 4.1 Bronze — raw landing (`notebooks/01_bronze_ingest.py`)

One Auto Loader streaming table per (source × lane). Five sources × two lanes
(NRT and historical) = ten bronze tables.

Bronze has no business logic. It only adds:
- `_ingested_at` — when DLT received the row
- `is_nrt` — lane flag (NRT=1, historical=0)
- `_source_file` — provenance
- `_rescued_data` — Auto Loader's bucket for malformed values

**Why this matters**: bronze is your replay buffer. If silver/gold logic has
a bug, you re-derive from bronze without re-fetching from the source. That's
the medallion contract.

### 4.2 Silver — cleaned and conformed (`notebooks/02_silver_transform.py`)

- Union NRT + historical lanes (`_unioned_bronze`)
- Type coercion (`to_timestamp`, lowercase enums)
- DQX validation (split rows into valid + quarantine)
- SCD applied via `apply_changes`
- Schema-aware modeling: dim_customer (SCD2), dim_meter (SCD1), dim_geography
  (batch), fact_readings (streaming append)

### 4.3 Gold — business KPIs (`notebooks/03_gold_kpis.py`)

Joins silver tables, aggregates by dimensions, applies DQX, writes to
`gold.kpi_*`. Three KPIs: peak demand ratio, grid stability index, climate
impact factor.

### 4.4 Why three layers and not two

Tradeoff: you could collapse silver+gold for simple cases. We kept three
because:
- DQX runs at silver and gold (two enforcement points catch different problems)
- Silver dim/fact tables are reused by multiple gold KPIs — caching them in
  Delta avoids recomputing joins
- Backfills replay one layer at a time (small blast radius)

---

## Part 5: Auto Loader (`cloudFiles`)

Spark Structured Streaming's file source on steroids. Configured in
`notebooks/01_bronze_ingest.py`:

```python
spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .option("cloudFiles.inferColumnTypes", "true")
    .load(source_path)
```

### 5.1 Schema inference + evolution

Auto Loader samples files, infers a schema, and persists it under
`schemaLocation`. When new fields appear in incoming JSON, it adds them
automatically (`addNewColumns` mode). Malformed values land in `_rescued_data`
instead of crashing the stream.

**Tradeoff**: convenience vs strictness. In a regulated environment you'd pin
a schema explicitly. For a smart-meter project where source schema drifts,
evolution is the right default. Available modes:
- `addNewColumns` — what we use; permissive
- `failOnNewColumns` — strict; pipeline halts on new fields
- `rescue` — silently capture into `_rescued_data`
- `none` — no evolution

### 5.2 The "missing prefix" pitfall

Auto Loader needs the source S3 prefix to **exist** at pipeline start. We hit:

```
java.io.FileNotFoundException: No such file or directory:
s3://bkt-.../raw/nrt/smart_meters
```

The historical seeder only wrote `raw/historical/`. The NRT prefix didn't
exist because the cron was paused. Fix: bootstrap with one tick (`make
seed-nrt` runs the simulator once).

**Lesson**: Auto Loader requires the directory, not just future writes to it.
Plan for cold-start.

---

## Part 6: SCD with `apply_changes`

DLT has built-in SCD support. No hand-written MERGE statements.

### 6.1 SCD2 — `dim_customer`

```python
dlt.create_streaming_table(name=f"{CATALOG}.silver.dim_customer", ...)

dlt.apply_changes(
    target=f"{CATALOG}.silver.dim_customer",
    source="dim_customer_valid_v",
    keys=["account_id"],
    sequence_by=col("sequence_struct"),
    stored_as_scd_type=2,
    track_history_except_column_list=[
        "_ingested_at", "_source_file", "_rescued_data",
        "sequence_struct", "is_nrt",
    ],
)
```

Output table gets `__START_AT`, `__END_AT` columns. A row is "current" when
`__END_AT IS NULL`. Gold uses this to filter to current state:

```python
customers = dlt.read(f"{CATALOG}.silver.dim_customer").filter("__END_AT IS NULL")
```

### 6.2 The `sequence_by` ordering trick

Source data has two lanes: historical (potentially stale) and NRT (fresh).
When both lanes report the same key, NRT should win.

```python
struct(
    col("is_nrt"),  # NRT=1 > historical=0 → NRT wins ties
    col(event_time_col).alias("event_time"),
    col("_ingested_at"),
)
```

Three-tier sort: lane > event time > ingestion time. Composing into a struct
lets `sequence_by` use lexicographic ordering across all three without
custom comparators.

**Rejected alternative**: a single timestamp column with NRT events nudged
forward by epsilon. Conflates lane semantics with time and breaks if event
timestamps are imprecise.

### 6.3 `track_history_except_column_list`

Without this list, every change to `_ingested_at` (which changes on every
batch) would create a new SCD2 row. The exclusion list says: "treat changes to
these columns as non-historical" — they update in place. Always exclude
technical/metadata columns from SCD2 tracking.

### 6.4 SCD1 — `dim_meter`

SCD1 also goes through `apply_changes`, with `stored_as_scd_type=1`:

```python
dlt.create_streaming_table(name=f"{CATALOG}.silver.dim_meter", ...)
dlt.apply_changes(
    target=f"{CATALOG}.silver.dim_meter",
    source="dim_meter_valid_v",
    keys=["meter_id"],
    sequence_by=col("sequence_struct"),
    stored_as_scd_type=1,
)
```

Latest row per `meter_id` wins, ordered by `sequence_struct` (NRT-flag,
event-time, ingestion-time). No `__START_AT` / `__END_AT` columns are added —
that's the SCD1 vs SCD2 difference.

A bare `@dlt.table` reading from `dim_meter_valid_v` would NOT do this — it
just appends every valid row. `apply_changes` is what gives you the upsert.

---

## Part 7: Data quality with DQX

`databricks-labs-dqx` is a Databricks Labs library for declarative DQ. Rules
in YAML, applied at runtime.

### 7.1 The valid/quarantine split

Each silver and gold table has a YAML rule file. At runtime:

```python
valid, quarantine = engine.apply_checks_by_metadata_and_split(df, rules)
```

- `error` criticality → row dropped to quarantine
- `warn` criticality → row stays in valid but is annotated

Both halves get written to separate Delta tables (`silver.dim_customer` +
`silver.dim_customer_quarantine`). You keep auditability without polluting
downstream.

### 7.2 Don't run DQX twice — share via views

Naive: define `dim_customer` and `dim_customer_quarantine` as separate
`@dlt.table`s, each running DQX. Doubles compute.

Pattern we adopted (`_checked_view` helper in silver):

```python
@dlt.view(name=f"{table}_valid_v")
def _valid():
    valid, _ = engine.apply_checks_by_metadata_and_split(...)
    return valid

@dlt.view(name=f"{table}_quarantine_v")
def _quarantine():
    _, quarantine = engine.apply_checks_by_metadata_and_split(...)
    return quarantine

# Consumers read from the views; DQX runs once per source
```

DLT recognizes shared computation under views and avoids double-execution.
Views are the right abstraction for shared in-pipeline work.

### 7.3 API gotchas

DQX is young — check release notes when upgrading. We hit:

```yaml
# wrong (older API)
arguments:
  column: kwh
  min: 0.0
  max: 1000.0

# right
arguments:
  column: kwh
  min_limit: 0.0
  max_limit: 1000.0
```

The error message clearly listed expected args, but the docs we used were
out of date.

---

## Part 8: The dual-lane backfill pattern

Smart-grid data arrives two ways: historical (one big drop, one-time) and
NRT (every 15 min forever). The pipeline handles both.

### 8.1 Two bronze tables per source, one silver

Bronze loop in `01_bronze_ingest.py`:

```python
for _table in SOURCE_TABLES:
    _bronze_table(_table, lane="nrt")          # raw_<table>
    _bronze_table(_table, lane="historical")   # raw_<table>_historical
```

Silver `_unioned_bronze` chooses behavior based on a flag:

```python
def _unioned_bronze(table):
    nrt = _bronze(table, "nrt")
    if not MERGE_HISTORICAL:
        return nrt
    hist = _bronze(table, "historical")
    return nrt.unionByName(hist, allowMissingColumns=True)
```

### 8.2 The lifecycle

There are two phases the project lives in. The transition between them is a
**one-time event**, but it has a non-obvious operational step that's easy to
miss the first time.

| # | Step | Command | What happens |
|---|---|---|---|
| 1 | Seed S3 | `make seed` | Historical files + 1 NRT bootstrap tick written to `s3://.../raw/...` |
| 2 | Backfill deploy | `make deploy-backfill` | `terraform apply -var merge_historical=true` → pipeline `configuration.merge_historical = "true"` |
| 3 | Backfill run | `make pipeline-run` | `--full-refresh` builds silver/gold from `NRT ∪ historical` (a **2-source** streaming plan, with a 2-source checkpoint) |
| 4 | Verify | `make verify` | `SHOW TABLES` across `bronze`/`silver`/`gold` |
| 5 | Steady-state deploy | `make deploy` | `terraform apply` (default `merge_historical=false`) → pipeline config flips to `"false"` |
| 6 | **Phase-transition refresh** | `make pipeline-run` | `--full-refresh` rewrites the streaming checkpoints from the 2-source shape to the 1-source shape (NRT only) |
| 7 | Steady state | (NRT cron, every 15 min) | NRT bronze appends → silver/gold update incrementally; no further `terraform apply` or `pipeline-run` needed |

Step 6 is the step we missed on the first build. Without it the next pipeline
trigger fails with a streaming-checkpoint mismatch — see Part 8.5 for why.

**Rejected alternative**: backfill into NRT (write historical files to
`raw/nrt/...`).
- Loses lineage (can't tell what came from where)
- Auto Loader would re-process them on schema changes
- The lane flag in `is_nrt` powers SCD2 ordering

### 8.3 How the parameter reaches the notebook

Pipeline configuration:

```hcl
# terraform/pipeline.tf
configuration = {
  "merge_historical" = tostring(var.merge_historical)
  ...
}
```

```python
# notebooks/02_silver_transform.py
MERGE_HISTORICAL = spark.conf.get("merge_historical", "false").lower() == "true"
```

Terraform sets it; notebook reads via `spark.conf.get`. This is the canonical
way to inject runtime parameters into DLT — the same notebook runs in dev and
prod with different configurations.

### 8.4 The flip — what actually changes when you toggle the flag

Walking through every layer the value passes through, from your laptop to the
streaming query that uses it:

1. **Terraform variable** (`terraform/variables.tf`)
   ```hcl
   variable "merge_historical" {
     type    = bool
     default = false   # steady-state default
   }
   ```
   The default is `false`. `deploy-backfill` overrides with `-var merge_historical=true`; `deploy` uses the default.
2. **Pipeline configuration map** (`terraform/pipeline.tf`)
   ```hcl
   configuration = {
     "merge_historical" = tostring(var.merge_historical)
     ...
   }
   ```
   Terraform serializes the bool to a string (DLT configuration values are always strings).
3. **REST API call.** `terraform apply` issues `PUT /api/2.0/pipelines/<id>` with the new spec. Diff in the plan looks like:
   ```
   ~ configuration = {
       ~ "merge_historical" = "true" -> "false"
     }
   ```
   No notebook re-upload, no schema change — just a metadata mutation on the pipeline definition. Idempotent and seconds-fast.
4. **Pipeline trigger.** Nothing happens yet. The new value takes effect only on the **next** pipeline run (cron, manual, or `make pipeline-run`).
5. **Notebook reads it back.** `spark.conf.get("merge_historical", "false")` returns the latest value at run time. `_unioned_bronze` branches on it (notebooks/02_silver_transform.py:22).

**Rule:** the flip is a server-side config mutation. The local `.tf` files are
just the source of truth Terraform synchronizes from. You can verify the flip
landed by opening the DLT pipeline → Settings → Configuration in the UI.

### 8.5 The streaming checkpoint — and why the next run fails if you skip the refresh

Spark Structured Streaming (which DLT uses under the hood for streaming tables)
maintains a **checkpoint** for each query: a small Delta-backed log that
records the source plan, source offsets, watermarks, and state-store handles.

The checkpoint is *bound to the structure of the streaming plan that wrote
it.* Two relevant invariants:

- **Number of sources is fixed.** A query with `nrt UNION historical` has 2 streaming sources. The checkpoint serializes offsets for source 0 and source 1.
- **Source ordering is fixed.** Source 0 is always source 0; you can't swap them.

When you flip `merge_historical: true → false`, `_unioned_bronze` returns a
single-source `nrt` DataFrame instead of a two-source union. DLT builds a
fresh streaming query from that, opens the existing checkpoint, and Spark's
`MicroBatchExecution` immediately throws:

```
assertion failed: There are [2] sources in the checkpoint offsets
and now there are [1] sources requested by the query. Cannot continue.
```

This is **not corruption** and **not a bug in DLT**. It's Spark refusing to
silently mutate the checkpoint shape, because the alternative — auto-trimming
offsets — would produce undefined semantics (which historical offset do you
keep? which do you drop?). The engine's stance: "the structure changed; you
must explicitly tell me how to handle the existing checkpoint."

Every silver streaming table that consumes `_unioned_bronze` is affected
simultaneously: `dim_customer`, `dim_meter`, `fact_readings`, and their
quarantine peers. Every gold table that depends on those silver tables shows
SKIPPED → "upstream failure" in the DAG. Per Part 12, those gold errors are
**cascade victims**, not the root cause.

### 8.6 The fix: full refresh as a phase-transition operation

`databricks pipelines start-update --full-refresh`:

- Truncates each streaming table.
- Deletes its checkpoint directory.
- Rebuilds from the bronze sources, writing a fresh checkpoint in the new
  (1-source) shape.

This is exactly the **medallion replay contract** from Part 4: bronze is the
durable buffer; silver and gold are derivable from it. Re-deriving them is
cheap because bronze is unchanged on S3.

For our project that's a single `make pipeline-run`. Running it once after
`make deploy` (step 6 in the lifecycle table) is the entire phase-transition
operation. From that point on, the 15-min NRT cron continues incrementally;
no further `--full-refresh` is needed unless you intentionally rebuild.

**Surgical alternative.** For larger pipelines where re-reading every bronze
file is not free, full-refresh only the streaming tables whose plan shape
changed:

```sh
databricks pipelines start-update "$PID" \
  --full-refresh-selection dim_customer,dim_meter,fact_readings \
  --refresh-selection ''
```

Bronze Auto Loader checkpoints stay intact; only silver streaming checkpoints
get rebuilt. We don't bother for 5 small sources, but it's the right tool when
the bronze re-read becomes expensive.

**Caveat — continuous DLT pipelines.** Pipelines configured as `continuous`
(rather than triggered, which is what we use) **cannot be selectively
full-refreshed** through the UI. The workaround in production codebases is a
"migration YAML" harness that drops a one-shot reset directive into a special
folder; a separate notebook reads that folder and applies the refresh on
restart. Out of scope here, but worth knowing if you ever switch this
project to continuous mode.

### 8.7 Why this design despite the rough edge

The 2→1 source flip is the cost of a clean dual-lane design. The cost is paid
exactly **once** in the project's lifetime (one phase transition), and the
resulting steady state is simple: one streaming source, one checkpoint, no
historical-bronze reads on every micro-batch.

The alternative designs all have permanent costs:

| Alternative | Why we didn't | Permanent cost |
|---|---|---|
| Always 2-source plan (union with empty historical leg) | Semantics get muddied: silver always "thinks" it has two lanes even after historical is folded in | Doubled checkpoint bookkeeping; planning overhead per micro-batch |
| Backfill at bronze only (write historical into `raw/nrt/...`) | Loses lane provenance; Auto Loader could re-process on schema changes | Loses the `is_nrt` flag that powers SCD2 ordering |
| Backfill via separate one-shot batch job | Two distinct code paths to silver | Code duplication; backfill bypasses DQX |

This isn't unique to our pipeline. Production multi-tenant data platforms in
the industry use the same `merge_historical`-style flag with the same manual
flip — and document the post-flip refresh as **"still all manual"**. The
rough edge is a Spark-engine constraint that ripples up into every dual-lane
design, not something the smart-meter project did poorly.

**Where to improve.** A `make graduate` target that bundles step 5
(`terraform apply` for steady state) and step 6 (`pipeline-run --full-refresh`)
into a single verb would close the documentation gap. The flip is naturally a
one-step operation in the user's mind ("I'm graduating from backfill to
steady state"); the current Makefile makes it look like two unrelated steps.

---

## Part 9: Spark Streaming Checkpoints — the concept that explains half the errors

Part 8.5 introduced checkpoints to explain the source-count mismatch. This
section zooms in on the checkpoint mechanism itself — what it is, what it
stores, how DLT wraps it, and why it matters for every streaming table in the
pipeline.

### 9.1 What problem checkpoints solve

Streaming queries run forever (or at least across many triggered micro-batches).
Between runs, the engine needs to remember:

- **Where did I leave off?** Which files has Auto Loader already processed?
  What's the latest offset for each source?
- **What was my plan?** How many sources feed this query, in what order?
- **What state am I carrying?** Watermarks, deduplication state, session
  windows.

Without this record, every restart would re-process the entire source from
scratch — correctness lost, duplicates everywhere. The checkpoint is Spark's
answer: a durable, append-only log that records exactly what has been
processed.

### 9.2 What lives inside a checkpoint

A checkpoint is a directory (on S3, DBFS, or the DLT-managed metastore) with
this structure:

```
checkpoint/
├── metadata          # query ID, serialized logical plan
├── offsets/          # one file per micro-batch
│   ├── 0             # batch 0: starting offsets for each source
│   ├── 1             # batch 1: offsets after batch 0 committed
│   └── ...
├── commits/          # one file per successfully committed batch
│   ├── 0
│   ├── 1
│   └── ...
├── sources/          # per-source state (e.g. Auto Loader file lists)
│   ├── 0/            # source 0 (e.g. NRT Auto Loader)
│   └── 1/            # source 1 (e.g. historical Auto Loader)
└── state/            # operator state (watermarks, session windows, etc.)
```

Key things to note:

- **`offsets/`** records are JSON. Each file lists one offset per source,
  indexed by position (source 0, source 1, …). This is why the source count
  is baked into the checkpoint — the offset array length IS the source count.
- **`commits/`** tracks which batches completed end-to-end. If a batch appears
  in `offsets/` but not in `commits/`, Spark re-executes it on restart
  (exactly-once semantics).
- **`sources/`** is source-type-specific. For Auto Loader, it stores the list
  of files already ingested so they aren't re-read.

### 9.3 The per-batch lifecycle

Here's what happens on each triggered micro-batch for a silver streaming table
like `dim_customer`:

```
┌─────────────────────────────────────────────────────────┐
│ 1. Open checkpoint, read latest committed offset        │
│ 2. Ask each source: "what's new since that offset?"     │
│    ├── Source 0 (NRT Auto Loader): 3 new files → read   │
│    └── Source 1 (historical Auto Loader): 0 new files   │
│ 3. Write new offset to offsets/N                        │
│ 4. Execute the micro-batch (transforms, DQX, writes)    │
│ 5. Write commit marker to commits/N                     │
│ 6. Update watermark in state/                           │
└─────────────────────────────────────────────────────────┘
```

Steps 3 and 5 are the exactly-once guarantee: if the job crashes between 3
and 5, the next restart sees the offset but no commit, so it re-executes the
batch. If it crashes after 5, the next restart sees the commit and moves on.

**The cost per source**: even when a source has zero new data (like the
historical leg after backfill), Spark still:
- Reads its latest offset from the checkpoint
- Asks the source for new data (Auto Loader does a `listFiles` on S3)
- Writes its unchanged offset into the new offset file

This is the "checkpoint overhead" mentioned in Part 8.7. For Auto Loader on
a static S3 prefix, it's a single `ListObjectsV2` call — milliseconds. At
production scale with hundreds of tables or sub-second latency SLAs, those
milliseconds add up. At our scale (5 tables, 15-min cron), it's irrelevant.

### 9.4 How DLT manages checkpoints (so you don't have to)

In vanilla Spark Structured Streaming, you manage checkpoint paths yourself:

```python
# Vanilla Spark — you own the checkpoint path
query = (
    df.writeStream
    .option("checkpointLocation", "s3://bucket/checkpoints/my_query")
    .table("silver.dim_customer")
)
```

DLT abstracts this away entirely. When you write:

```python
@dlt.table(name="dev_smart_grid.silver.dim_customer")
def dim_customer():
    return dlt.read_stream("int_customer_accounts")
```

DLT:
1. Allocates a checkpoint path in its internal storage (you never see or
   configure it)
2. Manages the lifecycle — creates on first run, reopens on incremental runs,
   deletes on `--full-refresh`
3. Maps each `dlt.read_stream` call to a numbered source in the checkpoint

This is why `--full-refresh` fixes checkpoint issues: DLT deletes the entire
checkpoint directory and rebuilds from scratch. You can't (and shouldn't) go
delete checkpoint files manually in DLT — the abstraction owns them.

### 9.5 The invariants Spark enforces

Spark's checkpoint has two hard invariants that cannot be violated without a
full reset:

| Invariant | What it means | Violation |
|---|---|---|
| **Source count is fixed** | The offset array length is set on the first batch. Every subsequent batch must have the same number of sources. | Adding or removing a source (e.g. flipping `merge_historical` from 2→1) breaks the checkpoint. |
| **Source order is fixed** | Source 0 is always source 0. You can't reorder them. | Swapping two `unionByName` operands changes which source gets which offset — silent data loss. |

These aren't arbitrary restrictions. The offset log is a positional array, not
a named map. Spark has no way to know that "source 1 was removed" vs "source 0
was removed and source 1 should become source 0." Rather than guess, it fails
hard. This is the correct engineering choice — silent data loss is worse than a
loud failure.

### 9.6 Practical implications for this project

| Scenario | Checkpoint impact | What to do |
|---|---|---|
| Normal NRT run (cron every 15 min) | Checkpoint advances incrementally. Each batch processes only new files. | Nothing — this is the steady state. |
| `--full-refresh` | DLT deletes all checkpoints. Every streaming table re-reads from the start of its source. | Use when you need to rebuild silver/gold from bronze (schema change, logic fix, source-count change). |
| `--full-refresh-selection table1,table2` | Only named tables' checkpoints are deleted. Others continue incrementally. | Surgical fix when only specific tables need a reset. Bronze Auto Loader checkpoints stay intact. |
| Adding a new silver table | New checkpoint created. Existing checkpoints unaffected. | No special action needed. |
| Changing transform logic (same sources) | Checkpoint is fine — it tracks offsets, not transform logic. Next batch applies new code to new data. | Incremental run picks up changes. Full refresh only if you want to reprocess historical data with new logic. |

### 9.7 Checkpoint vs Auto Loader state — two different things

It's easy to confuse these because they both track "what's been processed":

| | Streaming checkpoint | Auto Loader state |
|---|---|---|
| **Scope** | Per streaming query (one per silver/gold table) | Per Auto Loader source (one per bronze table) |
| **Stored at** | DLT-managed internal path | `cloudFiles.schemaLocation` on S3 (`s3://bucket/_schema/raw_*`) |
| **Tracks** | Offsets across all sources, watermarks, state | Which files have been listed/ingested from one S3 prefix |
| **Deleted by `--full-refresh`** | Yes | Yes — Auto Loader re-reads all files from the prefix |
| **Managed by** | DLT (you never touch it) | Auto Loader (configured in `01_bronze_ingest.py`) |

When we say "full refresh re-reads everything from S3," it's because **both**
are reset: Auto Loader forgets which files it's seen, and the streaming
checkpoint forgets which batches it's committed. The combination means every
file is re-ingested into bronze and re-processed into silver/gold.

### 9.8 The mental model

Think of the checkpoint as a **bookmark in a book**. The book is your data
source (S3 files). The bookmark tracks where you stopped reading.

- **Normal run**: move the bookmark forward as you read new pages.
- **Full refresh**: throw away the bookmark, start from page 1.
- **Source-count change**: the book now has a different number of chapters.
  Your old bookmark is indexing the wrong chapter. Spark won't guess which
  chapter mapping is correct — it asks you to start fresh.

This is the concept that connects Part 5 (Auto Loader), Part 8 (dual-lane
backfill), and Part 12 (debugging). Most DLT streaming errors trace back to
a checkpoint invariant being violated or a stale checkpoint after a structural
change.

---

## Part 10: Infrastructure as Code with Terraform

### 10.1 Why Terraform here

The Databricks UI lets you click together a pipeline, but reproducing it
across environments (dev/staging/prod) requires either careful exports or
IaC. We chose IaC from day one. Resources are git-tracked, code-reviewable,
and reversible.

### 10.2 Key resources

| Resource | What it does |
|---|---|
| `databricks_catalog` | Creates a Unity Catalog with optional `storage_root` |
| `databricks_schema` | Creates `bronze`/`silver`/`gold` under the catalog |
| `databricks_external_location` | Registers an S3 prefix as a UC external location |
| `databricks_pipeline` | Defines the DLT pipeline (libraries, configuration, target) |
| `databricks_job` | Defines a regular Databricks job (used here for the NRT cron) |
| `databricks_notebook` | Uploads a `.py` file as a workspace **notebook** |
| `databricks_workspace_file` | Uploads any file to the workspace |

### 10.3 The notebook-vs-file distinction (we got this wrong first)

DLT pipelines can only run **notebooks**, not raw `.py` files. They look the
same on disk but Databricks treats them differently — notebooks have language
metadata. Our initial code used `databricks_workspace_file` for everything,
and the pipeline failed:

> Failed to load notebook... Only SQL and Python notebooks are supported.
> UNSUPPORTED_LANGUAGE

Fix: switch the three pipeline source files to `databricks_notebook`:

```hcl
resource "databricks_notebook" "bronze_notebook" {
  source   = "${path.module}/../notebooks/01_bronze_ingest.py"
  path     = "${local.workspace_root}/notebooks/01_bronze_ingest"  # no .py
  language = "PYTHON"
}
```

Notebook paths drop the `.py` extension. The provider hashes the source file
and re-uploads automatically when you change it locally — that's how
`make deploy-backfill` keeps the workspace in sync with your edits.

**Rule of thumb**: if a file is referenced by `library { notebook { path = ... }}`
in a pipeline or job, use `databricks_notebook`. Everything else (helper
Python modules, config, DQX YAMLs) uses `databricks_workspace_file`.

### 10.4 Configuration block

Pipeline parameters travel via the `configuration` block:

```hcl
configuration = {
  "bucket_url"     = var.bucket_url
  "merge_historical" = tostring(var.merge_historical)
  "dq_rules_path"  = "${local.workspace_root}/data_quality"
  "target_catalog" = local.catalog_name
}
```

Notebooks read these with `spark.conf.get("name")`. Decouples code from
environment.

### 10.5 What `terraform apply` actually does for this project

1. Creates/updates the catalog and schemas
2. Uploads notebooks via the Workspace API
3. Creates/updates the DLT pipeline definition
4. Creates/updates the cron job for NRT simulation

It does **not**:
- Trigger the pipeline (you do that with `make pipeline-run`)
- Move data
- Touch S3 (apart from registering external locations)

Apply is fast and idempotent — most edits update one resource and leave the
rest alone.

### 10.6 How `terraform apply` actually pushes code — the REST API underneath

The Terraform Databricks provider is a thin Go shim over the **Databricks
REST API**. Every Terraform resource maps to one or more API endpoints, and
`terraform apply` makes ordinary HTTP calls authenticated with your
`databricks` CLI profile (or `DATABRICKS_HOST` + `DATABRICKS_TOKEN`).

| Terraform resource | API endpoint(s) called |
|---|---|
| `databricks_notebook` | `POST /api/2.0/workspace/import` (uploads file content, base64-encoded) |
| `databricks_workspace_file` | `POST /api/2.0/workspace/import` (same endpoint, `format=AUTO`) |
| `databricks_pipeline` | `POST /api/2.0/pipelines` (create) or `PUT /api/2.0/pipelines/{id}` (update) |
| `databricks_catalog` | `POST /api/2.1/unity-catalog/catalogs` |
| `databricks_schema` | `POST /api/2.1/unity-catalog/schemas` |
| `databricks_job` | `POST /api/2.1/jobs/create` or `reset` |

**The notebook upload, in detail.** For `databricks_notebook.bronze_notebook`:

1. Provider reads `notebooks/01_bronze_ingest.py` from your laptop
2. Base64-encodes the bytes
3. POSTs roughly this JSON to `/api/2.0/workspace/import`:
   ```json
   {
     "path": "/Workspace/Shared/dev_smart_grid/notebooks/01_bronze_ingest",
     "language": "PYTHON",
     "format": "SOURCE",
     "overwrite": true,
     "content": "<base64 of your .py file>"
   }
   ```
4. Databricks decodes and writes it to that workspace path as a notebook

**The pipeline definition.** For `databricks_pipeline.smart_grid`, the
provider POSTs the entire pipeline spec — libraries, configuration, target
schema, channel — as JSON. Databricks stores the definition; nothing runs
yet. `make pipeline-run` is what calls `POST /api/2.0/pipelines/{id}/updates`
to actually trigger execution.

**State and idempotency.** Terraform stores resource IDs in
`terraform.tfstate`. Next run, it `GET`s each resource, diffs against your
`.tf`, and only `PUT`s the ones that changed. The notebook resource hashes
the local source file — if the hash is unchanged, no API call. That's why
editing a single notebook and running `make deploy-backfill` only re-uploads
that one file.

**Auth.** The provider authenticates the same way as the `databricks` CLI
(profile, OAuth, or PAT). Same credentials, same API surface — Terraform is
just one client among many.

**Verifying the API calls yourself.** Set `TF_LOG=DEBUG` to see every HTTP
request:

```
TF_LOG=DEBUG terraform -chdir=terraform apply 2>&1 | grep -E "(POST|PUT|GET) /api"
```

You'll see the exact endpoints hit for each resource. Anything Terraform
does, you can reproduce with `curl` against the same endpoints — the UI,
CLI, SDKs, and Terraform are all clients of one REST surface.

---

## Part 11: Operational ergonomics — the Makefile

Rather than memorizing `terraform`, `databricks`, `aws`, and `python`
invocations, we wrapped the lifecycle in `make`:

```
make install          # venv + deps + unit tests
make seed             # historical + NRT bootstrap into S3
make deploy-backfill  # terraform apply with merge_historical=true
make pipeline-run     # databricks pipelines start-update --full-refresh
make verify           # SHOW TABLES across schemas
make deploy           # steady-state apply
make smoke            # integration test
```

Make is just a task runner here — no makefile dependency-graph magic. The
value is documentation: `make help` (or just reading the file) tells anyone
the deploy sequence.

**Why make and not a Python wrapper**:
- Deploy steps ARE shell commands; no need to wrap them
- `make help` self-documents
- Cross-platform between macOS/Linux developer machines

---

## Part 12: Debugging DLT pipelines

The pipeline UI's Event Log is the only tool that matters. Habits that saved
us hours:

1. **Sort by time ascending.** Find the root cause, not the cascade.
2. **Filter to ERROR.** Skip info noise.
3. **Click the failed node in the DAG.** Gives you the specific dataset and
   the line of code.

### 12.1 Common error patterns we hit

| Error | Real cause |
|---|---|
| `Dataset 'X' is defined in the pipeline but could not be resolved` | Upstream dataset failed; X is the cascade victim. Hunt upstream. |
| `View 'X' is a streaming view and must be referenced using readStream` | Consumer used `dlt.read` on a `_unioned_bronze`-derived view |
| `View 'X' is not a streaming view and must be referenced using read` | Consumer used `dlt.read_stream` on a batch view (like `int_dim_geography`) |
| `assertion failed: There are [2] sources in the checkpoint offsets and now there are [1] sources requested by the query. Cannot continue.` | `merge_historical` was flipped from `true` to `false` without a follow-up `pipeline-run --full-refresh`. The streaming checkpoint still records the 2-source plan; the freshly built 1-source plan refuses to mutate it. See Part 8.5–8.6. Fix: `make pipeline-run`. |
| `FileNotFoundException: No such file or directory: s3://.../raw/nrt/X` | Auto Loader prefix doesn't exist; bootstrap with at least one file |
| `Only SQL and Python notebooks are supported. UNSUPPORTED_LANGUAGE` | Used `databricks_workspace_file` instead of `databricks_notebook` |
| `Workspace doesn't support Client-1 channel for REPL` | Free Edition rejects `client = "1"`; use `"2"` |
| `Unexpected argument 'min' for function 'is_in_range'` | DQX renamed args to `min_limit`/`max_limit` |
| `Metastore storage root URL does not exist` | Free Edition needs explicit `storage_root` on the catalog |

---

## Part 13: Free Edition specifics

Brief because they're version-dependent:

- **Serverless only** — can't define classic clusters
- **One pipeline per type** — single DLT pipeline per workspace
- **Default storage requires `storage_root`** — TF provider doesn't auto-discover
- **Client version `"2"`** — `"1"` is rejected
- **No account-level APIs** — workspace-level only

If you have paid Databricks, ignore this section.

---

## Part 14: What I'd do differently

Reflecting on the build:

1. **Start with one Auto Loader prefix populated end-to-end before scaling
   out.** We deployed all 10 bronze tables and only then discovered the
   missing-prefix issue. Cheaper to hit it on one stream.
2. **Adopt `databricks_notebook` from the start.** The workspace-file mistake
   cost a deploy round-trip.
3. **Model batch vs streaming view boundaries upfront.** The `_checked_view`
   `streaming` flag could have been there from day one if we'd designed the
   geography (batch) path explicitly instead of retrofitting.
4. **Lean harder on the DLT event log timeline.** Reading the second/third
   error first wastes time; always go to the earliest error.
5. **Document the `merge_historical` two-phase deploy in the README earlier.**
   It's a subtle lifecycle that's easy to forget.

---

## Glossary

- **DLT** — Lakeflow Declarative Pipelines (formerly Delta Live Tables)
- **DPM** — Direct Publishing Mode, DLT's mode requiring fully-qualified names
- **DQX** — `databricks-labs-dqx`, the data quality library
- **NRT** — Near-Real-Time (the 15-min cron lane in this project)
- **SCD** — Slowly Changing Dimension (Type 1 = overwrite; Type 2 = history)
- **UC** — Unity Catalog
- **Workspace path** — the `/Workspace/...` filesystem inside Databricks
- **Control plane** — Databricks-hosted UI/scheduler/metastore
- **Compute plane** — VMs running your code (yours or Databricks-managed)
- **Auto Loader** — `cloudFiles` source for incremental file ingestion
- **Apply changes** — DLT's built-in CDC/SCD merge primitive
