# ingestion/nrt/simulate_nrt_feed.py
"""NRT feed simulator. Each invocation appends a small batch of rows per source
to s3://.../raw/nrt/<table>/dt=YYYY-MM-DD/<table>_<UTC-ts>_0000.json.

Designed to be triggered on a cron (every 15 min). Auto Loader on the bronze
streaming tables picks up the new files automatically.
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timezone

from ingestion._common import (
    DEFAULT_BUCKET,
    SOURCE_TABLES,
    gen_customers,
    gen_events,
    gen_meters,
    gen_readings,
    gen_weather,
    make_context,
    nrt_object_key,
    write_jsonl_to_s3,
)

log = logging.getLogger("nrt_simulator")


def generate_tick(
    bucket: str,
    ts: datetime | None = None,
    rows_per_table: int = 100,
    seed: int | None = None,
) -> dict[str, int]:
    """Write one batch per source. Returns {table: row_count}."""
    if ts is None:
        ts = datetime.now(timezone.utc)
    if seed is None:
        seed = int(ts.timestamp())  # different seed per tick → fresh data

    ctx = make_context(seed=seed)
    counts: dict[str, int] = {}

    customers = gen_customers(ctx, n=rows_per_table, with_dupes=0)
    counts["customer_accounts"] = write_jsonl_to_s3(
        customers, bucket, nrt_object_key("customer_accounts", ts=ts, part=0)
    )

    account_ids = [c["account_id"] for c in customers if c.get("account_id")]
    meters = gen_meters(ctx, n=max(1, rows_per_table // 2), account_ids=account_ids, with_dupes=0)
    counts["smart_meters"] = write_jsonl_to_s3(
        meters, bucket, nrt_object_key("smart_meters", ts=ts, part=0)
    )

    weather, _coords = gen_weather(
        ctx, n_stations=10, n_readings=rows_per_table, with_dupes=0
    )
    counts["weather_station"] = write_jsonl_to_s3(
        weather, bucket, nrt_object_key("weather_station", ts=ts, part=0)
    )

    events = gen_events(ctx, n=rows_per_table, with_dupes=0)
    counts["grid_events"] = write_jsonl_to_s3(
        events, bucket, nrt_object_key("grid_events", ts=ts, part=0)
    )

    meter_ids = [m["meter_id"] for m in meters]
    readings = gen_readings(ctx, n=rows_per_table * 5, meter_ids=meter_ids, with_dupes=0)
    counts["meter_readings"] = write_jsonl_to_s3(
        readings, bucket, nrt_object_key("meter_readings", ts=ts, part=0)
    )

    return counts


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", default=DEFAULT_BUCKET)
    p.add_argument("--rows-per-table", type=int, default=100)
    args = p.parse_args()

    counts = generate_tick(bucket=args.bucket, rows_per_table=args.rows_per_table)
    for table in SOURCE_TABLES:
        log.info("nrt tick wrote %d rows for %s", counts[table], table)


if __name__ == "__main__":
    main()
