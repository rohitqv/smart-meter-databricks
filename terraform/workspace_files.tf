locals {
  workspace_root = "/Workspace/Shared/${local.catalog_name}"
}

resource "databricks_workspace_file" "historical_loader" {
  source = "${path.module}/../ingestion/historical/load_historical.py"
  path   = "${local.workspace_root}/ingestion/historical/load_historical.py"
}

resource "databricks_workspace_file" "common" {
  source = "${path.module}/../ingestion/_common.py"
  path   = "${local.workspace_root}/ingestion/_common.py"
}

resource "databricks_workspace_file" "ingestion_init" {
  source = "${path.module}/../ingestion/__init__.py"
  path   = "${local.workspace_root}/ingestion/__init__.py"
}

resource "databricks_workspace_file" "historical_init" {
  source = "${path.module}/../ingestion/historical/__init__.py"
  path   = "${local.workspace_root}/ingestion/historical/__init__.py"
}

resource "databricks_notebook" "bronze_notebook" {
  source   = "${path.module}/../notebooks/01_bronze_ingest.py"
  path     = "${local.workspace_root}/notebooks/01_bronze_ingest"
  language = "PYTHON"
}

resource "databricks_workspace_file" "nrt_simulator" {
  source = "${path.module}/../ingestion/nrt/simulate_nrt_feed.py"
  path   = "${local.workspace_root}/ingestion/nrt/simulate_nrt_feed.py"
}

resource "databricks_workspace_file" "nrt_init" {
  source = "${path.module}/../ingestion/nrt/__init__.py"
  path   = "${local.workspace_root}/ingestion/nrt/__init__.py"
}

resource "databricks_notebook" "silver_notebook" {
  source   = "${path.module}/../notebooks/02_silver_transform.py"
  path     = "${local.workspace_root}/notebooks/02_silver_transform"
  language = "PYTHON"
}

# DQX rule files — uploaded as workspace files for the notebooks to read at runtime.
resource "databricks_workspace_file" "dq_silver" {
  for_each = fileset("${path.module}/../data_quality/silver", "*.yaml")
  source   = "${path.module}/../data_quality/silver/${each.value}"
  path     = "${local.workspace_root}/data_quality/silver/${each.value}"
}

# Datadog helper modules — uploaded as workspace files so DLT notebooks can import them.
resource "databricks_workspace_file" "datadog_metrics" {
  source = "${path.module}/../notebooks/lib/datadog_metrics.py"
  path   = "${local.workspace_root}/notebooks/lib/datadog_metrics.py"
}

resource "databricks_workspace_file" "dlt_hooks" {
  source = "${path.module}/../notebooks/lib/dlt_hooks.py"
  path   = "${local.workspace_root}/notebooks/lib/dlt_hooks.py"
}

resource "databricks_notebook" "gold_notebook" {
  source   = "${path.module}/../notebooks/03_gold_kpis.py"
  path     = "${local.workspace_root}/notebooks/03_gold_kpis"
  language = "PYTHON"
}

resource "databricks_workspace_file" "dq_gold" {
  for_each = fileset("${path.module}/../data_quality/gold", "*.yaml")
  source   = "${path.module}/../data_quality/gold/${each.value}"
  path     = "${local.workspace_root}/data_quality/gold/${each.value}"
}
