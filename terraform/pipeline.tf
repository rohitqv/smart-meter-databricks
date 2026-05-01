resource "databricks_pipeline" "smart_grid" {
  name    = "${local.catalog_name}_pipeline"
  catalog = databricks_catalog.smart_grid.name
  target  = "" # leave empty — use UC notation in the notebooks (catalog.schema.table)

  serverless = true
  channel    = "CURRENT"
  edition    = "ADVANCED"

  configuration = {
    "bucket_url"       = var.bucket_url
    "merge_historical" = "false" # steady-state: NRT only
    "dq_rules_path"    = "${local.workspace_root}/data_quality"
    "target_catalog"   = local.catalog_name
  }

  library {
    notebook {
      path = databricks_workspace_file.bronze_notebook.path
    }
  }

  library {
    notebook {
      path = databricks_workspace_file.silver_notebook.path
    }
  }

  library {
    notebook {
      path = databricks_workspace_file.gold_notebook.path
    }
  }

  depends_on = [databricks_schema.schemas]
}
