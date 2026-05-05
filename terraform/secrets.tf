# ── Databricks secret scopes for Datadog credentials ─────────────────────────
# Pipeline code reads these with dbutils.secrets.get(scope, key).

resource "databricks_secret_scope" "datadog" {
  name = "datadog"
}

resource "databricks_secret" "dd_api_key" {
  scope        = databricks_secret_scope.datadog.name
  key          = "dd_api_key"
  string_value = var.dd_api_key
}

resource "databricks_secret" "dd_app_key" {
  scope        = databricks_secret_scope.datadog.name
  key          = "dd_app_key"
  string_value = var.dd_app_key
}

resource "databricks_secret_scope" "resources" {
  name = "resources"
}

resource "databricks_secret" "client_name" {
  scope        = databricks_secret_scope.resources.name
  key          = "client_name"
  string_value = "smart_grid"
}

resource "databricks_secret" "environment" {
  scope        = databricks_secret_scope.resources.name
  key          = "environment"
  string_value = var.env
}
