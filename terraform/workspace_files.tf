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

resource "databricks_workspace_file" "bronze_notebook" {
  source = "${path.module}/../notebooks/01_bronze_ingest.py"
  path   = "${local.workspace_root}/notebooks/01_bronze_ingest.py"
}
