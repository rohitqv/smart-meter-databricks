# Databricks notebook source
# notebooks/02_silver_transform.py
# MAGIC %pip install databricks-labs-dqx
# COMMAND ----------
import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, lit, struct, to_timestamp, trim, upper, lower,
    when, regexp_extract, regexp_replace,
)

CATALOG = spark.conf.get("target_catalog")
MERGE_HISTORICAL = spark.conf.get("merge_historical", "false").lower() == "true"
DQ_RULES_PATH = spark.conf.get("dq_rules_path")


def _bronze(table: str, lane: str = "nrt") -> DataFrame:
    name = f"raw_{table}_historical" if lane == "historical" else f"raw_{table}"
    return dlt.read_stream(f"{CATALOG}.bronze.{name}")


def _unioned_bronze(table: str) -> DataFrame:
    """Union NRT + historical when MERGE_HISTORICAL=True; else NRT only."""
    nrt = _bronze(table, "nrt")
    if not MERGE_HISTORICAL:
        return nrt
    hist = _bronze(table, "historical")
    return nrt.unionByName(hist, allowMissingColumns=True)


def _unioned_bronze_batch(table: str) -> DataFrame:
    """Batch (non-streaming) read of unioned bronze. Used by consumers that need
    a snapshot for dropDuplicates/crossJoin (int_dim_geography, gold KPIs)."""
    nrt = dlt.read(f"{CATALOG}.bronze.raw_{table}")
    if not MERGE_HISTORICAL:
        return nrt
    hist = dlt.read(f"{CATALOG}.bronze.raw_{table}_historical")
    return nrt.unionByName(hist, allowMissingColumns=True)


def _add_sequence_struct(df: DataFrame, event_time_col: str) -> DataFrame:
    """Build the SCD2 ordering struct: (NRT-flag, event-time, ingestion-time).
    NRT wins ties against historical because is_nrt=1 > 0.
    """
    return df.withColumn(
        "sequence_struct",
        struct(
            col("is_nrt"),
            col(event_time_col).alias("event_time"),
            col("_ingested_at"),
        ),
    )


# ---------------------------------------------------------------------------
# int_* staging tier — one per source
# ---------------------------------------------------------------------------

@dlt.view(name="int_customer_accounts")
def int_customer_accounts():
    df = _unioned_bronze("customer_accounts")
    return _add_sequence_struct(
        df.withColumn("created_at_ts", to_timestamp(col("created_at"))),
        event_time_col="created_at_ts",
    )


@dlt.view(name="int_smart_meters")
def int_smart_meters():
    df = _unioned_bronze("smart_meters")
    return _add_sequence_struct(
        df.withColumn("installation_date_parsed", to_timestamp(col("installation_date"), "yyyy-MM-dd")),
        event_time_col="installation_date_parsed",
    )


@dlt.view(name="int_meter_readings")
def int_meter_readings():
    df = _unioned_bronze("meter_readings")
    return _add_sequence_struct(
        df.withColumn("reading_ts", to_timestamp(col("timestamp"))),
        event_time_col="reading_ts",
    )


def _grid_events_canonicalize(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("event_ts", to_timestamp(col("timestamp")))
        .withColumn("event_type_canonical", lower(col("event_type")))
    )


def _weather_canonicalize(df: DataFrame) -> DataFrame:
    return df.withColumn("recorded_ts", to_timestamp(col("recorded_at")))


@dlt.view(name="int_weather_station")
def int_weather_station():
    df = _unioned_bronze("weather_station")
    return _add_sequence_struct(_weather_canonicalize(df), event_time_col="recorded_ts")


@dlt.view(name="int_weather_station_batch")
def int_weather_station_batch():
    """Batch counterpart of int_weather_station for gold KPI consumers."""
    return _weather_canonicalize(_unioned_bronze_batch("weather_station"))


@dlt.view(name="int_grid_events")
def int_grid_events():
    df = _unioned_bronze("grid_events")
    return _add_sequence_struct(_grid_events_canonicalize(df), event_time_col="event_ts")


@dlt.view(name="int_grid_events_batch")
def int_grid_events_batch():
    """Batch counterpart of int_grid_events for gold KPI consumers."""
    return _grid_events_canonicalize(_unioned_bronze_batch("grid_events"))


# ---------------------------------------------------------------------------
# DQX helpers — shared by all silver tables
# ---------------------------------------------------------------------------
import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient


def _load_rules(layer: str, table: str) -> list[dict]:
    path = f"{DQ_RULES_PATH}/{layer}/{table}.yaml"
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _checked_view(source_view: str, layer: str, table: str, transform=lambda df: df, streaming: bool = True):
    """Define two views from one DQX run, named <table>_valid_v and <table>_quarantine_v.
    Consumer tables read from these to avoid running DQX twice. The `_v` suffix
    avoids name collisions with the consumer @dlt.table definitions.

    streaming=False is required when the source view is a batch view (e.g.,
    int_dim_geography), since DLT rejects dlt.read_stream on a batch view.
    """
    engine = DQEngine(WorkspaceClient())
    rules = _load_rules(layer, table)
    read = dlt.read_stream if streaming else dlt.read

    @dlt.view(name=f"{table}_valid_v")
    def _valid():
        df = transform(read(source_view))
        valid, _ = engine.apply_checks_by_metadata_and_split(df, rules)
        return valid

    @dlt.view(name=f"{table}_quarantine_v")
    def _quarantine():
        df = transform(read(source_view))
        _, quarantine = engine.apply_checks_by_metadata_and_split(df, rules)
        return quarantine.withColumn("_quarantined_at", current_timestamp())


# ---------------------------------------------------------------------------
# dim_customer (SCD2) — sources the valid half; quarantine half goes to its own table
# ---------------------------------------------------------------------------

def _customer_transform(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("name", trim(col("name")))
        .withColumn("service_type_canonical", lower(col("service_type")))
    )


_checked_view(
    source_view="int_customer_accounts",
    layer="silver",
    table="dim_customer",
    transform=_customer_transform,
)

dlt.create_streaming_table(
    name=f"{CATALOG}.silver.dim_customer",
    comment="Silver dim_customer — SCD Type 2",
)

dlt.apply_changes(
    target=f"{CATALOG}.silver.dim_customer",
    source="dim_customer_valid_v",
    keys=["account_id"],
    sequence_by=col("sequence_struct"),
    stored_as_scd_type=2,
    track_history_except_column_list=[
        "_ingested_at",
        "_source_file",
        "_rescued_data",
        "sequence_struct",
        "is_nrt",
    ],
)


@dlt.table(
    name=f"{CATALOG}.silver.dim_customer_quarantine",
    comment="Quarantine sink for silver.dim_customer (DQX failures)",
    table_properties={"quality": "quarantine"},
)
def dim_customer_quarantine():
    return dlt.read_stream("dim_customer_quarantine_v")


# ---------------------------------------------------------------------------
# dim_meter (SCD1 — latest version per meter_id)
# ---------------------------------------------------------------------------

def _meter_transform(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "model_canonical",
        upper(regexp_replace(col("model"), "_", "-")),
    )


_checked_view(
    source_view="int_smart_meters",
    layer="silver",
    table="dim_meter",
    transform=_meter_transform,
)


@dlt.table(name=f"{CATALOG}.silver.dim_meter", comment="Silver dim_meter — SCD Type 1")
def dim_meter():
    return dlt.read_stream("dim_meter_valid_v")


@dlt.table(
    name=f"{CATALOG}.silver.dim_meter_quarantine",
    comment="Quarantine sink for silver.dim_meter",
    table_properties={"quality": "quarantine"},
)
def dim_meter_quarantine():
    return dlt.read_stream("dim_meter_quarantine_v")


# ---------------------------------------------------------------------------
# dim_geography (constructed from weather_station + customer zip)
# ---------------------------------------------------------------------------

@dlt.view(name="int_dim_geography")
def _int_dim_geography():
    weather = (
        _unioned_bronze_batch("weather_station")
        .select(col("station_id"), col("lat"), col("lon"))
        .dropDuplicates(["station_id"])
    )
    customer_zips = (
        _unioned_bronze_batch("customer_accounts")
        .select(col("zip_code"))
        .filter(col("zip_code").isNotNull())
        .dropDuplicates()
    )
    # Cartesian-style enrichment is OK at this scale; for production use a proper geo-join.
    return customer_zips.crossJoin(weather)


_checked_view(
    source_view="int_dim_geography",
    layer="silver",
    table="dim_geography",
    streaming=False,
)


@dlt.table(name=f"{CATALOG}.silver.dim_geography", comment="Silver dim_geography — SCD Type 1")
def dim_geography():
    return dlt.read("dim_geography_valid_v")  # batch (non-streaming) — geography rebuilt each run


@dlt.table(name=f"{CATALOG}.silver.dim_geography_quarantine", comment="Quarantine sink for silver.dim_geography")
def dim_geography_quarantine():
    return dlt.read("dim_geography_quarantine_v")


# ---------------------------------------------------------------------------
# fact_readings
# ---------------------------------------------------------------------------

def _readings_transform(df: DataFrame) -> DataFrame:
    return df.withColumn("status_code_canonical", lower(col("status_code")))


_checked_view(
    source_view="int_meter_readings",
    layer="silver",
    table="fact_readings",
    transform=_readings_transform,
)


@dlt.table(name=f"{CATALOG}.silver.fact_readings", comment="Silver fact_readings — one row per meter per reading_ts")
def fact_readings():
    return dlt.read_stream("fact_readings_valid_v")


@dlt.table(name=f"{CATALOG}.silver.fact_readings_quarantine", comment="Quarantine sink for silver.fact_readings")
def fact_readings_quarantine():
    return dlt.read_stream("fact_readings_quarantine_v")
