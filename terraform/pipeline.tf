resource "databricks_pipeline" "smart_grid" {
  name    = "${local.catalog_name}_pipeline"
  catalog = databricks_catalog.smart_grid.name
  target  = "" # leave empty — use UC notation in the notebooks (catalog.schema.table)

  serverless = true
  channel    = "CURRENT"
  edition    = "ADVANCED"

  configuration = {
    "bucket_url"        = var.bucket_url
    "merge_historical"  = "true"   # flip to "false" after initial backfill
    "dq_rules_path"     = "${local.workspace_root}/data_quality"
    "target_catalog"    = local.catalog_name
  }

  library {
    notebook {
      path = databricks_workspace_file.bronze_notebook.path
    }
  }

  # silver + gold notebooks added in Tasks 22 and 28 (uncomment when ready):
  # library {
  #   notebook {
  #     path = databricks_workspace_file.silver_notebook.path
  #   }
  # }
  # library {
  #   notebook {
  #     path = databricks_workspace_file.gold_notebook.path
  #   }
  # }

  depends_on = [databricks_schema.schemas]
}
