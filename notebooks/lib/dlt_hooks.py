"""DLT event hook factories for emitting Datadog metrics.

These hooks register via @dlt.on_event_hook and fire on flow-progress events
in continuous-mode DLT pipelines. For batch-mode pipelines, call
upload_to_datadog() directly after pipeline completion instead.

Usage from a DLT notebook:
    from lib.dlt_hooks import set_pipeline_delay_hook, set_heartbeat_hook
    set_pipeline_delay_hook("dev_smart_grid", spark, dbutils)
    set_heartbeat_hook("dev_smart_grid", spark, dbutils)
"""

import time

import dlt

from lib.datadog_metrics import upload_to_datadog

# Track first-seen time per flow so we can report elapsed seconds
_flow_first_seen: dict[str, float] = {}


def set_pipeline_delay_hook(fallback_catalog: str, spark, dbutils) -> None:
    """Register a DLT event hook that emits pipeline delay metrics."""
    dd_api_key = dbutils.secrets.get(scope="datadog", key="dd_api_key")
    dd_app_key = dbutils.secrets.get(scope="datadog", key="dd_app_key")
    client_name = dbutils.secrets.get(scope="resources", key="client_name")

    @dlt.on_event_hook
    def pipeline_delay_hook(event: dict) -> None:
        if event.get("event_type") != "flow_progress":
            return

        flow_name = event["origin"]["flow_name"]
        update_id = event["origin"]["update_id"]

        now = time.time()
        start = _flow_first_seen.setdefault(flow_name, now)
        duration = now - start

        parts = flow_name.split(".")
        catalog = parts[0] if len(parts) >= 2 else fallback_catalog
        schema = parts[1] if len(parts) >= 2 else "unknown"
        environment = catalog.split("_")[0]

        upload_to_datadog(
            flow_name, duration, update_id, client_name, environment,
            dd_api_key, dd_app_key, catalog, schema,
            metric_name="smart_grid.dlt.pipeline_delay",
        )


def set_heartbeat_hook(fallback_catalog: str, spark, dbutils) -> None:
    """Emit a heartbeat on every flow start — powers 'is this alive?' alerts."""
    dd_api_key = dbutils.secrets.get(scope="datadog", key="dd_api_key")
    dd_app_key = dbutils.secrets.get(scope="datadog", key="dd_app_key")
    client_name = dbutils.secrets.get(scope="resources", key="client_name")

    @dlt.on_event_hook
    def heartbeat_hook(event: dict) -> None:
        if event.get("event_type") != "flow_progress":
            return

        flow_name = event["origin"]["flow_name"]
        update_id = event["origin"]["update_id"]
        environment = fallback_catalog.split("_")[0]

        upload_to_datadog(
            flow_name, 1.0, update_id, client_name, environment,
            dd_api_key, dd_app_key, fallback_catalog, "heartbeat",
            metric_name="smart_grid.dlt.heartbeat",
        )
