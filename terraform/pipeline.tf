resource "databricks_pipeline" "smart_grid" {
  name    = "${local.catalog_name}_pipeline"
  catalog = databricks_catalog.smart_grid.name
  target  = "bronze" # default schema for unqualified table names; silver/gold notebooks use fully-qualified names

  serverless = true
  channel    = "CURRENT"
  edition    = "ADVANCED"

  configuration = {
    "bucket_url"       = var.bucket_url
    "merge_historical" = tostring(var.merge_historical)
    "dq_rules_path"    = "${local.workspace_root}/data_quality"
    "target_catalog"   = local.catalog_name
    "log_datadog"      = var.log_datadog
  }

  library {
    notebook {
      path = databricks_notebook.bronze_notebook.path
    }
  }

  library {
    notebook {
      path = databricks_notebook.silver_notebook.path
    }
  }

  library {
    notebook {
      path = databricks_notebook.gold_notebook.path
    }
  }

  depends_on = [databricks_schema.schemas]
}
