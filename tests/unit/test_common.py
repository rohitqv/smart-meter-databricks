# tests/unit/test_common.py
from datetime import datetime, timezone

import pytest

from ingestion._common import (
    SOURCE_TABLES,
    historical_object_key,
    nrt_object_key,
)


def test_source_tables_are_the_five_we_expect():
    assert set(SOURCE_TABLES) == {
        "meter_readings",
        "weather_station",
        "customer_accounts",
        "smart_meters",
        "grid_events",
    }


def test_historical_object_key_uses_fixed_naming():
    # Re-running historical generation must overwrite the same paths (idempotent).
    key = historical_object_key("meter_readings", part=0)
    assert key == "raw/historical/meter_readings/meter_readings_historical_0000.json"


def test_historical_object_key_zero_pads_part_to_four_digits():
    key = historical_object_key("meter_readings", part=12)
    assert key.endswith("meter_readings_historical_0012.json")


def test_nrt_object_key_includes_date_partition_and_utc_timestamp():
    ts = datetime(2026, 5, 1, 14, 30, 45, tzinfo=timezone.utc)
    key = nrt_object_key("meter_readings", ts=ts, part=0)
    assert key == "raw/nrt/meter_readings/dt=2026-05-01/meter_readings_20260501T143045Z_0000.json"


def test_nrt_object_key_rejects_naive_datetimes():
    with pytest.raises(ValueError, match="UTC"):
        nrt_object_key("meter_readings", ts=datetime(2026, 5, 1, 14, 30, 45), part=0)
