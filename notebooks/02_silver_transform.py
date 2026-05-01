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
    return dlt.read_stream(name)


def _unioned_bronze(table: str) -> DataFrame:
    """Union NRT + historical when MERGE_HISTORICAL=True; else NRT only."""
    nrt = _bronze(table, "nrt")
    if not MERGE_HISTORICAL:
        return nrt
    hist = _bronze(table, "historical")
    return nrt.unionByName(hist, allowMissingColumns=True)


def _add_sequence_struct(df: DataFrame, event_time_col: str) -> DataFrame:
    """Build the SCD2 ordering struct: (NRT-flag, event-time, ingestion-time).
    NRT wins ties against historical because is_not_historical_data=1 > 0.
    """
    return df.withColumn(
        "sequence_struct",
        struct(
            col("is_not_historical_data"),
            col(event_time_col).alias("file_effective_datetime"),
            col("_ingested_at"),
        ),
    )


# ---------------------------------------------------------------------------
# int_* staging tier — one per source
# ---------------------------------------------------------------------------

@dlt.view(name="int_customer_scd2_vw")
def int_customer_scd2_vw():
    df = _unioned_bronze("customer_accounts")
    return _add_sequence_struct(
        df.withColumn("created_at_ts", to_timestamp(col("created_at"))),
        event_time_col="created_at_ts",
    )


@dlt.view(name="int_smart_meter_vw")
def int_smart_meter_vw():
    df = _unioned_bronze("smart_meters")
    return _add_sequence_struct(
        df.withColumn("installation_date_parsed", to_timestamp(col("installation_date"), "yyyy-MM-dd")),
        event_time_col="installation_date_parsed",
    )


@dlt.view(name="int_meter_readings_vw")
def int_meter_readings_vw():
    df = _unioned_bronze("meter_readings")
    return _add_sequence_struct(
        df.withColumn("reading_ts", to_timestamp(col("timestamp"))),
        event_time_col="reading_ts",
    )


@dlt.view(name="int_weather_station_vw")
def int_weather_station_vw():
    df = _unioned_bronze("weather_station")
    return _add_sequence_struct(
        df.withColumn("recorded_ts", to_timestamp(col("recorded_at"))),
        event_time_col="recorded_ts",
    )


@dlt.view(name="int_grid_events_vw")
def int_grid_events_vw():
    df = _unioned_bronze("grid_events")
    return _add_sequence_struct(
        df.withColumn("event_ts", to_timestamp(col("timestamp"))),
        event_time_col="event_ts",
    )


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


def _checked_view(source_view: str, layer: str, table: str, transform=lambda df: df):
    """Define two views from one DQX run, named <table>_valid_v and <table>_quarantine_v.
    Consumer tables read from these to avoid running DQX twice. The `_v` suffix
    avoids name collisions with the consumer @dlt.table definitions.
    """
    engine = DQEngine(WorkspaceClient())
    rules = _load_rules(layer, table)

    @dlt.view(name=f"{table}_valid_v")
    def _valid():
        df = transform(dlt.read_stream(source_view))
        valid, _ = engine.apply_checks_by_metadata_and_split(df, rules)
        return valid

    @dlt.view(name=f"{table}_quarantine_v")
    def _quarantine():
        df = transform(dlt.read_stream(source_view))
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
    source_view="int_customer_scd2_vw",
    layer="silver",
    table="dim_customer",
    transform=_customer_transform,
)

dlt.create_streaming_table(
    name="dim_customer",
    comment="Silver dim_customer — SCD Type 2",
)

dlt.apply_changes(
    target="dim_customer",
    source="dim_customer_valid_v",
    keys=["account_id"],
    sequence_by=col("sequence_struct"),
    stored_as_scd_type=2,
    track_history_except_column_list=["_ingested_at", "_source_file", "_rescued_data"],
)


@dlt.table(
    name="dim_customer_quarantine",
    comment="Quarantine sink for silver.dim_customer (DQX failures)",
    table_properties={"quality": "quarantine"},
)
def dim_customer_quarantine():
    return dlt.read_stream("dim_customer_quarantine_v")
