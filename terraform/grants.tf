data "databricks_current_user" "me" {}

resource "databricks_grants" "catalog_owner" {
  catalog = databricks_catalog.smart_grid.name
  grant {
    principal  = data.databricks_current_user.me.user_name
    privileges = ["ALL_PRIVILEGES"]
  }
  grant {
    principal  = "f6acb8ea-0a5c-4ed3-9ff1-86f80ce630fb"
    privileges = ["USE_CATALOG", "USE_SCHEMA", "SELECT"]
  }
}

# NOTE: system catalog grants cannot be managed via Terraform on Free Edition.
# The workspace user does not have MANAGE on the system catalog (owned by Databricks).
# If cost visibility via Datadog is needed, grant manually from the SQL Warehouse:
#   GRANT USE CATALOG ON CATALOG system TO `f6acb8ea-0a5c-4ed3-9ff1-86f80ce630fb`;
#   GRANT SELECT ON CATALOG system TO `f6acb8ea-0a5c-4ed3-9ff1-86f80ce630fb`;
#   GRANT USE SCHEMA ON CATALOG system TO `f6acb8ea-0a5c-4ed3-9ff1-86f80ce630fb`;
# This may also fail on Free Edition — system catalog access may be restricted entirely.
