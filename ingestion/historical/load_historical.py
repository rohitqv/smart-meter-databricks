# ingestion/historical/load_historical.py
"""One-shot historical loader: generates Faker data and writes fixed-name
JSON files into s3://.../raw/historical/<table>/.

Re-running this script overwrites the same keys (idempotent — no duplicates).
Usage: python -m ingestion.historical.load_historical
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

# When Databricks runs this as a workspace file (interactive "Run" or
# spark_python_task), only the script's directory ends up on sys.path, so
# `import ingestion` fails. Add the project root (two levels up) explicitly.
# The interactive launcher runs the file in a globals dict with no `__file__`,
# so fall back to the compiled code object's filename via the current frame.
try:
    _THIS_FILE = __file__
except NameError:
    _THIS_FILE = sys._getframe(0).f_code.co_filename
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(_THIS_FILE))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from ingestion._common import (
    DEFAULT_BUCKET,
    SOURCE_TABLES,
    gen_customers,
    gen_events,
    gen_meters,
    gen_readings,
    gen_weather,
    historical_object_key,
    make_context,
    write_jsonl_to_s3,
)

log = logging.getLogger("historical_loader")

ROWS_PER_FILE = 10_000  # split readings into multiple files for Auto Loader parallelism


def run(bucket: str, seed: int = 42) -> dict[str, int]:
    """Generate all five tables and write to S3. Returns {table: row_count}."""
    ctx = make_context(seed=seed)
    counts: dict[str, int] = {}

    log.info("generating customer_accounts")
    customers = gen_customers(ctx, n=2000, with_dupes=50)
    counts["customer_accounts"] = write_jsonl_to_s3(
        customers, bucket, historical_object_key("customer_accounts", part=0)
    )

    account_ids = [c["account_id"] for c in customers if c.get("account_id")]
    log.info("generating smart_meters")
    meters = gen_meters(ctx, n=1500, account_ids=account_ids, with_dupes=30)
    counts["smart_meters"] = write_jsonl_to_s3(
        meters, bucket, historical_object_key("smart_meters", part=0)
    )

    log.info("generating weather_station")
    weather, _coords = gen_weather(ctx, n_stations=50, n_readings=520, with_dupes=20)
    counts["weather_station"] = write_jsonl_to_s3(
        weather, bucket, historical_object_key("weather_station", part=0)
    )

    log.info("generating grid_events")
    events = gen_events(ctx, n=3000, with_dupes=50)
    counts["grid_events"] = write_jsonl_to_s3(
        events, bucket, historical_object_key("grid_events", part=0)
    )

    log.info("generating meter_readings (split across files)")
    meter_ids = [m["meter_id"] for m in meters]
    readings = gen_readings(ctx, n=52000, meter_ids=meter_ids, with_dupes=2000)
    total = 0
    for part, start in enumerate(range(0, len(readings), ROWS_PER_FILE)):
        chunk = readings[start:start + ROWS_PER_FILE]
        total += write_jsonl_to_s3(
            chunk, bucket, historical_object_key("meter_readings", part=part)
        )
    counts["meter_readings"] = total

    return counts


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--bucket", default=DEFAULT_BUCKET)
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()

    counts = run(bucket=args.bucket, seed=args.seed)
    for table in SOURCE_TABLES:
        log.info("wrote %d rows for %s", counts[table], table)


if __name__ == "__main__":
    main()
