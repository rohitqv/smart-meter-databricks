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
      client       = "2"
      dependencies = ["faker>=24.0", "boto3[crt]>=1.34"]
    }
  }
}

resource "databricks_job" "nrt_simulator" {
  name = "${local.catalog_name}_nrt_ingestion_simulator"

  schedule {
    quartz_cron_expression = "0 */15 * * * ?" # every 15 minutes
    timezone_id            = "Asia/Kolkata"
    pause_status           = "PAUSED" # un-pause manually after first verify
  }

  task {
    task_key = "nrt_tick"

    spark_python_task {
      python_file = databricks_workspace_file.nrt_simulator.path
      parameters  = ["--rows-per-table", "100"]
    }

    environment_key = "default"
  }

  environment {
    environment_key = "default"
    spec {
      client       = "2"
      dependencies = ["faker>=24.0", "boto3[crt]>=1.34"]
    }
  }
}
