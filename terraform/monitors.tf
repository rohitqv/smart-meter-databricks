# ── Datadog monitors for Smart Grid pipeline ─────────────────────────────────
# Naming: [Smart Grid] short description - <env>
# Tags:   service:smart-grid, component:*, env:*, managed_by:terraform

resource "datadog_monitor" "pipeline_delay" {
  count = var.enable_monitoring ? 1 : 0

  name = "[Smart Grid] DLT pipeline delay - ${var.env}"
  type = "metric alert"

  query = "avg(last_15m):avg:smart_grid.dlt.pipeline_delay{env:${var.env}} > 600"

  message = <<-EOT
    {{#is_alert}}
    **P2**: Pipeline stage took > 10 min for `{{task_name.name}}` in ${var.env}.

    **Triage**
    1. Check the DLT pipeline run in Databricks UI.
    2. Look at quarantine table row counts for data quality issues.
    3. Check S3 source paths for missing or malformed files.
    {{/is_alert}}

    @slack-smart-grid-alerts
  EOT

  monitor_thresholds {
    warning  = 300
    critical = 600
  }

  priority = 2

  notify_no_data      = false
  renotify_interval   = 120
  require_full_window = false
  include_tags        = true

  tags = [
    "service:smart-grid",
    "component:dlt-pipeline",
    "env:${var.env}",
    "managed_by:terraform",
  ]
}

resource "datadog_monitor" "pipeline_heartbeat" {
  count = var.enable_monitoring ? 1 : 0

  name = "[Smart Grid] DLT pipeline silent - ${var.env}"
  type = "query alert"

  query = "sum(last_30m):sum:smart_grid.dlt.heartbeat{env:${var.env}}.as_count() < 1"

  message = <<-EOT
    {{#is_alert}}
    **P1**: No DLT heartbeats for 30 minutes in ${var.env}.
    Pipeline may be down or stuck.

    **Triage**
    1. Check Databricks Jobs & Pipelines UI — is the pipeline running?
    2. Check NRT simulator job — is it feeding data to S3?
    3. Manually trigger a pipeline update if needed.
    {{/is_alert}}

    @slack-smart-grid-alerts
  EOT

  notify_no_data    = true
  no_data_timeframe = 30
  priority          = 1

  tags = [
    "service:smart-grid",
    "component:dlt-pipeline",
    "env:${var.env}",
    "managed_by:terraform",
  ]
}

resource "datadog_monitor" "quarantine_spike" {
  count = var.enable_monitoring ? 1 : 0

  name = "[Smart Grid] DQX quarantine row spike - ${var.env}"
  type = "metric alert"

  query = "avg(last_15m):avg:smart_grid.dqx.quarantine_count{env:${var.env}} > 100"

  message = <<-EOT
    {{#is_alert}}
    **P2**: Quarantine table received > 100 rows in 15 min in ${var.env}.
    Possible data quality regression in upstream source.

    **Triage**
    1. Query the quarantine tables in gold/silver schemas.
    2. Check DQX rule YAML files for recently changed thresholds.
    3. Inspect raw S3 files for schema drift or corruption.
    {{/is_alert}}

    @slack-smart-grid-alerts
  EOT

  monitor_thresholds {
    warning  = 50
    critical = 100
  }

  priority = 2

  notify_no_data      = false
  renotify_interval   = 120
  require_full_window = false
  include_tags        = true

  tags = [
    "service:smart-grid",
    "component:data-quality",
    "env:${var.env}",
    "managed_by:terraform",
  ]
}
