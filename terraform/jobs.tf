resource "databricks_job" "historical_ingestion" {
  name = "${local.catalog_name}_historical_ingestion"

  task {
    task_key = "load_historical"

    spark_python_task {
      python_file = databricks_workspace_file.historical_loader.path
      parameters  = ["--bucket", replace(var.bucket_url, "s3://", "")]
    }

    environment_key = "default"
  }

  environment {
    environment_key = "default"
    spec {
      client       = "1"
      dependencies = ["faker>=24.0", "boto3>=1.34"]
    }
  }
}
