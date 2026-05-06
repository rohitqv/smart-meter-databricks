"""Datadog metric upload helper for DLT pipelines.

Usage from a DLT notebook:
    from lib.datadog_metrics import upload_to_datadog
"""

import time

from datadog_api_client import ApiClient, Configuration
from datadog_api_client.v2.api.metrics_api import MetricsApi
from datadog_api_client.v2.model.metric_intake_type import MetricIntakeType
from datadog_api_client.v2.model.metric_payload import MetricPayload
from datadog_api_client.v2.model.metric_point import MetricPoint
from datadog_api_client.v2.model.metric_resource import MetricResource
from datadog_api_client.v2.model.metric_series import MetricSeries

DEFAULT_METRIC = "smart_grid.dlt.pipeline_delay"


def upload_to_datadog(
    task_name: str,
    duration: float,
    update_id: str,
    client_name: str,
    environment: str,
    dd_api_key: str,
    dd_app_key: str,
    catalog: str,
    schema: str,
    timestamp: int | None = None,
    retry_count: int = 2,
    metric_name: str = DEFAULT_METRIC,
    resource_type: str = "dlt_flow",
) -> bool:
    """Upload a single gauge metric to Datadog with retries.

    Tags follow the production convention:
      client:, env:, task_name:, catalog:, schema:, update_id:.
    """
    cfg = Configuration()
    cfg.api_key["apiKeyAuth"] = dd_api_key
    cfg.api_key["appKeyAuth"] = dd_app_key

    ts = timestamp if timestamp else int(time.time())

    body = MetricPayload(series=[MetricSeries(
        metric=metric_name,
        type=MetricIntakeType.GAUGE,
        points=[MetricPoint(timestamp=ts, value=duration)],
        resources=[MetricResource(name=task_name, type=resource_type)],
        tags=[
            f"client:{client_name}",
            f"env:{environment}",
            f"task_name:{task_name.split('.')[-1]}",
            f"update_id:{update_id}",
            f"catalog:{catalog}",
            f"schema:{schema}",
        ],
    )])

    for attempt in range(retry_count + 1):
        try:
            with ApiClient(cfg) as api:
                MetricsApi(api).submit_metrics(body=body)
            return True
        except Exception:
            if attempt == retry_count:
                return False
    return False
