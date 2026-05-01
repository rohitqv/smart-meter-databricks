resource "databricks_catalog" "smart_grid" {
  name           = local.catalog_name
  comment        = "Smart-grid meter telemetry — env: ${var.env}"
  isolation_mode = "OPEN"
  storage_root   = "${var.bucket_url}/_uc_managed/${local.catalog_name}"
}

resource "databricks_schema" "schemas" {
  for_each     = toset(local.schemas)
  catalog_name = databricks_catalog.smart_grid.name
  name         = each.value
  comment      = "Smart-grid ${each.value} layer"
}
