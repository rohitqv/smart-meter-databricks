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
