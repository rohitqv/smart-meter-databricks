terraform {
  required_version = ">= 1.6"
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
    datadog = {
      source  = "datadog/datadog"
      version = "~> 3.40"
    }
  }
}

provider "databricks" {
  host = var.databricks_host
  # Auth resolves via DATABRICKS_TOKEN env var or ~/.databrickscfg
}

provider "datadog" {
  api_key = var.dd_api_key
  app_key = var.dd_app_key
}

locals {
  catalog_name = "${var.env}_${var.catalog_base_name}"
  schemas      = ["bronze", "silver", "gold", "quarantine"]
}
