variable "env" {
  description = "Deployment environment prefix (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "catalog_base_name" {
  description = "Catalog name suffix; full name becomes <env>_<catalog_base_name>"
  type        = string
  default     = "smart_grid"
}

variable "bucket_url" {
  description = "S3 bucket URL for raw data"
  type        = string
  default     = "s3://bkt-ry-smart-grid-meter-bucket"
}

variable "external_location_name" {
  description = "Pre-existing UC external location name"
  type        = string
  default     = "db_s3_external_databricks-s3-ingest-3c20a"
}

variable "databricks_host" {
  description = "Databricks workspace URL (e.g., https://xxx.cloud.databricks.com)"
  type        = string
}

variable "merge_historical" {
  description = "When true, the silver layer unions historical + NRT bronze tables. Set to true for the first pipeline run (backfill), then flip back to false for steady-state NRT-only operation."
  type        = bool
  default     = false
}

# ── Datadog integration ─────────────────────────────────────────────────────

variable "dd_api_key" {
  description = "Datadog API key"
  type        = string
  sensitive   = true
  default     = ""
}

variable "dd_app_key" {
  description = "Datadog application key"
  type        = string
  sensitive   = true
  default     = ""
}

variable "log_datadog" {
  description = "Datadog emission mode for DLT pipelines: false, batch, continuous"
  type        = string
  default     = "false"
}

variable "enable_monitoring" {
  description = "Enable Datadog monitors (set false for dev to avoid noise)"
  type        = bool
  default     = false
}
