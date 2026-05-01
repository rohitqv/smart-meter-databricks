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


from ingestion._common import (
    make_context,
    gen_customers, gen_meters, gen_weather, gen_events, gen_readings,
)


def test_generators_produce_expected_row_counts():
    ctx = make_context(seed=42)
    customers = gen_customers(ctx, n=100, with_dupes=5)
    assert len(customers) == 105

    account_ids = [c["account_id"] for c in customers if c.get("account_id")]
    meters = gen_meters(ctx, n=50, account_ids=account_ids, with_dupes=2)
    assert len(meters) == 52

    weather, coords = gen_weather(ctx, n_stations=10, n_readings=30, with_dupes=2)
    assert len(weather) == 32
    assert len(coords) == 10

    events = gen_events(ctx, n=20, with_dupes=2)
    assert len(events) == 22

    meter_ids = [m["meter_id"] for m in meters]
    readings = gen_readings(ctx, n=200, meter_ids=meter_ids, with_dupes=10)
    assert len(readings) == 210


def test_generators_are_reproducible_for_a_seed():
    a = gen_customers(make_context(seed=7), n=50, with_dupes=0)
    b = gen_customers(make_context(seed=7), n=50, with_dupes=0)
    assert a == b
