# Databricks notebook source
# notebooks/03_gold_kpis.py
# MAGIC %pip install databricks-labs-dqx
# COMMAND ----------
import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, expr, lit, max as F_max, mean as F_mean,
    sum as F_sum, to_date, when, window,
)

import yaml
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient

CATALOG = spark.conf.get("target_catalog")
DQ_RULES_PATH = spark.conf.get("dq_rules_path")


def _load_rules(layer: str, table: str) -> list[dict]:
    path = f"{DQ_RULES_PATH}/{layer}/{table}.yaml"
    with open(path, "r") as f:
        return yaml.safe_load(f)


def _checked_gold(source_view: str, table: str):
    """Same shape as silver's _checked_view but for batch gold sources.
    Creates <table>_valid_v and <table>_quarantine_v views; consumer tables read these.
    """
    engine = DQEngine(WorkspaceClient())
    rules = _load_rules("gold", table)

    @dlt.view(name=f"{table}_valid_v")
    def _valid():
        df = dlt.read(source_view)
        valid, _ = engine.apply_checks_by_metadata_and_split(df, rules)
        return valid

    @dlt.view(name=f"{table}_quarantine_v")
    def _quarantine():
        df = dlt.read(source_view)
        _, quarantine = engine.apply_checks_by_metadata_and_split(df, rules)
        return quarantine.withColumn("_quarantined_at", current_timestamp())


# ---------------------------------------------------------------------------
# kpi_peak_demand_ratio — peak vs avg load by zip per day
# ---------------------------------------------------------------------------

@dlt.view(name="int_kpi_peak_demand_ratio")
def _int_kpi_peak_demand_ratio():
    facts = dlt.read(f"{CATALOG}.silver.fact_readings").alias("f")
    meters = dlt.read(f"{CATALOG}.silver.dim_meter").alias("m")
    customers = dlt.read(f"{CATALOG}.silver.dim_customer").alias("c").filter("__END_AT IS NULL")  # current SCD2

    joined = (
        facts
        .join(meters, col("f.meter_id") == col("m.meter_id"))
        .join(customers, col("m.account_id") == col("c.account_id"))
        .withColumn("reading_date", to_date(col("f.reading_ts")))
        .filter(col("f.kwh").isNotNull() & (col("f.kwh") >= 0))
    )
    return (
        joined
        .groupBy(col("c.zip_code").alias("zip_code"), col("reading_date"))
        .agg(F_max("f.kwh").alias("peak_kwh"), F_mean("f.kwh").alias("avg_kwh"))
        .withColumn(
            "peak_demand_ratio",
            when(col("avg_kwh") > 0, col("peak_kwh") / col("avg_kwh")).otherwise(None),
        )
    )


_checked_gold("int_kpi_peak_demand_ratio", "kpi_peak_demand_ratio")


@dlt.table(name=f"{CATALOG}.gold.kpi_peak_demand_ratio", comment="Gold KPI — peak vs avg load by zip/day")
def kpi_peak_demand_ratio():
    return dlt.read("kpi_peak_demand_ratio_valid_v")


@dlt.table(name=f"{CATALOG}.gold.kpi_peak_demand_ratio_quarantine", comment="Quarantine for gold.kpi_peak_demand_ratio")
def kpi_peak_demand_ratio_quarantine():
    return dlt.read("kpi_peak_demand_ratio_quarantine_v")


# ---------------------------------------------------------------------------
# kpi_grid_stability_index — 1-hour rolling composite of voltage anomalies + outage count
# ---------------------------------------------------------------------------

@dlt.view(name="int_kpi_grid_stability_index")
def _int_kpi_grid_stability_index():
    facts = dlt.read(f"{CATALOG}.silver.fact_readings").alias("f")
    events = dlt.read("int_grid_events_batch_vw").alias("e")  # silver-notebook batch view

    voltage_flags = (
        facts
        .withColumn(
            "voltage_anomaly",
            when((col("voltage") < 110) | (col("voltage") > 240), 1).otherwise(0),
        )
        .groupBy(window(col("f.reading_ts"), "1 hour").alias("w"))
        .agg(F_mean("voltage_anomaly").alias("voltage_anomaly_rate"))
    )

    outage_counts = (
        events
        .filter(col("event_type").isin("power_outage", "POWER_OUTAGE"))
        .groupBy(window(col("e.event_ts"), "1 hour").alias("w"))
        .agg(F_sum(expr("1")).alias("outage_count"))
    )

    return (
        voltage_flags.join(outage_counts, "w", "left").na.fill(0)
        .withColumn("window_start", col("w.start"))
        .withColumn("window_end", col("w.end"))
        .withColumn("zip_code", lit("ALL"))  # v1: aggregate across all zips
        .withColumn(
            "stability_index",
            (1.0 - col("voltage_anomaly_rate")) * (1.0 / (1.0 + col("outage_count"))),
        )
        .drop("w")
    )


_checked_gold("int_kpi_grid_stability_index", "kpi_grid_stability_index")


@dlt.table(name=f"{CATALOG}.gold.kpi_grid_stability_index", comment="Gold KPI — hourly grid stability composite")
def kpi_grid_stability_index():
    return dlt.read("kpi_grid_stability_index_valid_v")


@dlt.table(name=f"{CATALOG}.gold.kpi_grid_stability_index_quarantine", comment="Quarantine for gold.kpi_grid_stability_index")
def kpi_grid_stability_index_quarantine():
    return dlt.read("kpi_grid_stability_index_quarantine_v")


# ---------------------------------------------------------------------------
# kpi_climate_impact_factor — load correlation with temperature
# ---------------------------------------------------------------------------

@dlt.view(name="int_kpi_climate_impact_factor")
def _int_kpi_climate_impact_factor():
    facts = dlt.read(f"{CATALOG}.silver.fact_readings").alias("f")
    customers = dlt.read(f"{CATALOG}.silver.dim_customer").alias("c").filter("__END_AT IS NULL")
    weather = dlt.read("int_weather_station_batch_vw").alias("w")  # silver-notebook batch view
    geography = dlt.read(f"{CATALOG}.silver.dim_geography").alias("g")
    meters = dlt.read(f"{CATALOG}.silver.dim_meter").alias("m")

    daily_load = (
        facts
        .join(meters, col("f.meter_id") == col("m.meter_id"))
        .join(customers, col("m.account_id") == col("c.account_id"))
        .withColumn("reading_date", to_date(col("f.reading_ts")))
        .groupBy(col("c.zip_code").alias("zip_code"), col("reading_date"))
        .agg(F_sum("f.kwh").alias("daily_kwh"))
    )

    daily_temp = (
        weather
        .withColumn("reading_date", to_date(col("w.recorded_ts")))
        .groupBy(col("w.station_id").alias("station_id"), col("reading_date"))
        .agg(F_mean("w.temp").alias("daily_avg_temp"))
        .join(geography.dropDuplicates(["station_id"]), "station_id")
        .select("zip_code", "reading_date", "daily_avg_temp")
        .dropDuplicates(["zip_code", "reading_date"])
    )

    return (
        daily_load.join(daily_temp, ["zip_code", "reading_date"], "inner")
        .withColumn(
            "climate_impact_factor",
            when(col("daily_avg_temp") != 0, col("daily_kwh") / col("daily_avg_temp")).otherwise(None),
        )
    )


_checked_gold("int_kpi_climate_impact_factor", "kpi_climate_impact_factor")


@dlt.table(name=f"{CATALOG}.gold.kpi_climate_impact_factor", comment="Gold KPI — daily load vs avg temp")
def kpi_climate_impact_factor():
    return dlt.read("kpi_climate_impact_factor_valid_v")


@dlt.table(name=f"{CATALOG}.gold.kpi_climate_impact_factor_quarantine", comment="Quarantine for gold.kpi_climate_impact_factor")
def kpi_climate_impact_factor_quarantine():
    return dlt.read("kpi_climate_impact_factor_quarantine_v")
