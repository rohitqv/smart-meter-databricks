data "databricks_current_user" "me" {}

resource "databricks_grants" "catalog_owner" {
  catalog = databricks_catalog.smart_grid.name
  grant {
    principal  = data.databricks_current_user.me.user_name
    privileges = ["ALL_PRIVILEGES"]
  }
}
