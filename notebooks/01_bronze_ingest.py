# Databricks notebook source
# notebooks/01_bronze_ingest.py
# MAGIC %pip install databricks-labs-dqx
# COMMAND ----------
import dlt
from pyspark.sql.functions import current_timestamp, lit

# Pipeline parameters (set on the DLT pipeline definition; see terraform/pipeline.tf)
BUCKET_URL = spark.conf.get("bucket_url")  # e.g., s3://bkt-ry-smart-grid-meter-bucket
CATALOG = spark.conf.get("target_catalog")

SOURCE_TABLES = [
    "meter_readings",
    "weather_station",
    "customer_accounts",
    "smart_meters",
    "grid_events",
]


def _bronze_table(table: str, lane: str) -> None:
    """Define one Auto Loader streaming bronze table.

    lane is "nrt" or "historical". Table name follows the spec:
      - nrt: bronze.raw_<table>
      - historical: bronze.raw_<table>_historical
    """
    if lane == "historical":
        bronze_name = f"raw_{table}_historical"
        source_path = f"{BUCKET_URL}/raw/historical/{table}/"
        is_not_historical = lit(0)
    elif lane == "nrt":
        bronze_name = f"raw_{table}"
        source_path = f"{BUCKET_URL}/raw/nrt/{table}/"
        is_not_historical = lit(1)
    else:
        raise ValueError(f"unknown lane: {lane}")

    schema_path = f"{BUCKET_URL}/_schema/{bronze_name}/"

    @dlt.table(
        name=f"{CATALOG}.bronze.{bronze_name}",
        comment=f"Bronze raw — {table} ({lane} lane)",
        table_properties={"quality": "bronze", "lane": lane},
    )
    def _():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", schema_path)
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("rescuedDataColumn", "_rescued_data")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(source_path)
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("is_not_historical_data", is_not_historical)
            .withColumn("_source_file", lit(source_path))
        )


for _table in SOURCE_TABLES:
    _bronze_table(_table, lane="nrt")
    _bronze_table(_table, lane="historical")
