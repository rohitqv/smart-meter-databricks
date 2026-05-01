# tests/integration/test_smoke.py
"""End-to-end smoke test against a live Databricks workspace.

Run manually after a successful pipeline update:
    pytest tests/integration/test_smoke.py -v

Requires DATABRICKS_HOST and DATABRICKS_TOKEN environment variables.
"""
import os

import pytest

pytestmark = pytest.mark.skipif(
    not os.environ.get("DATABRICKS_TOKEN"),
    reason="requires DATABRICKS_TOKEN; integration test only",
)

from databricks.sdk import WorkspaceClient

CATALOG = os.environ.get("SMART_GRID_CATALOG", "dev_smart_grid")


@pytest.fixture(scope="module")
def warehouse_id():
    w = WorkspaceClient()
    warehouses = list(w.warehouses.list())
    assert warehouses, "no SQL warehouses found in workspace"
    return warehouses[0].id


def _query(warehouse_id: str, sql: str):
    w = WorkspaceClient()
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=sql,
        wait_timeout="30s",
    )
    return resp.result.data_array if resp.result else []


@pytest.mark.parametrize("schema,expected_min", [
    ("bronze", 10),
    # silver holds dim/fact tables AND their *_quarantine pairs (see plan Task 19 cross-schema note):
    #   4 dim/fact + 4 quarantine = 8 tables minimum, plus 5 *_v views (not counted by SHOW TABLES)
    ("silver", 8),
    # gold: 3 KPI tables + 3 quarantine pairs = 6 tables minimum
    ("gold", 6),
])
def test_table_count_per_schema(warehouse_id, schema, expected_min):
    rows = _query(warehouse_id, f"SHOW TABLES IN {CATALOG}.{schema}")
    assert len(rows) >= expected_min, f"{schema} has fewer than {expected_min} tables: {rows}"


def test_fact_readings_has_data(warehouse_id):
    rows = _query(warehouse_id, f"SELECT COUNT(*) FROM {CATALOG}.silver.fact_readings")
    count = int(rows[0][0])
    assert count > 1000, f"fact_readings has only {count} rows"


def test_dim_customer_scd2_has_history(warehouse_id):
    rows = _query(
        warehouse_id,
        f"SELECT COUNT(*) FROM {CATALOG}.silver.dim_customer WHERE __END_AT IS NOT NULL",
    )
    count = int(rows[0][0])
    # Some accounts have duplicates with different timestamps → at least one closed version
    assert count > 0, "no closed SCD2 versions in dim_customer — expected at least one history row"


def test_quarantine_has_failures(warehouse_id):
    rows = _query(
        warehouse_id,
        f"SELECT COUNT(*) FROM {CATALOG}.silver.fact_readings_quarantine",
    )
    count = int(rows[0][0])
    assert count > 0, "no quarantined rows — DQX may not be running (intentional DQ issues should produce failures)"
