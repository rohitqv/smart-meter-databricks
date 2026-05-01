# ingestion/_common.py
"""Shared utilities for historical and NRT data generators.

Schemas are derived from the existing setup notebook
(`project-01-smart-grid-meter/01_smart_meter_data_setup.ipynb`); see that file
for the original Faker generator logic that this module ports to S3.
"""
from __future__ import annotations

import json
import os
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

import boto3
from faker import Faker

# ---------------------------------------------------------------------------
# Source registry
# ---------------------------------------------------------------------------

SOURCE_TABLES: tuple[str, ...] = (
    "meter_readings",
    "weather_station",
    "customer_accounts",
    "smart_meters",
    "grid_events",
)

DEFAULT_BUCKET = os.environ.get("SMART_GRID_BUCKET", "bkt-ry-smart-grid-meter-bucket")
DEFAULT_REGION = os.environ.get("SMART_GRID_REGION", "ap-south-1")

# ---------------------------------------------------------------------------
# S3 path helpers
# ---------------------------------------------------------------------------

def historical_object_key(table: str, part: int) -> str:
    """Fixed-name historical object key — re-runs overwrite in place."""
    return f"raw/historical/{table}/{table}_historical_{part:04d}.json"


def nrt_object_key(table: str, ts: datetime, part: int) -> str:
    """Timestamped NRT object key with hive-style date partition.

    `ts` MUST be timezone-aware UTC; naive datetimes raise ValueError so we
    don't accidentally write local-time paths into S3.
    """
    if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) != timedelta(0):
        raise ValueError("ts must be timezone-aware UTC")
    date_part = ts.strftime("%Y-%m-%d")
    ts_part = ts.strftime("%Y%m%dT%H%M%SZ")
    return f"raw/nrt/{table}/dt={date_part}/{table}_{ts_part}_{part:04d}.json"


# ---------------------------------------------------------------------------
# S3 client + writer
# ---------------------------------------------------------------------------

def s3_client():
    return boto3.client("s3", region_name=DEFAULT_REGION)


def write_jsonl_to_s3(records: Iterable[dict], bucket: str, key: str) -> int:
    """Write newline-delimited JSON to S3. Returns row count."""
    body = "\n".join(json.dumps(r) for r in records)
    s3_client().put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))
    return body.count("\n") + 1 if body else 0


# ---------------------------------------------------------------------------
# Faker generators (ported from setup notebook, with intentional DQ issues)
# ---------------------------------------------------------------------------

@dataclass
class GeneratorContext:
    fake: Faker
    rng: random.Random
    base_date: datetime  # naive, used for synthetic record timestamps


def make_context(seed: int = 42) -> GeneratorContext:
    fake = Faker()
    Faker.seed(seed)
    rng = random.Random(seed)
    return GeneratorContext(
        fake=fake,
        rng=rng,
        base_date=datetime(2024, 1, 1),
    )


def gen_customers(ctx: GeneratorContext, n: int = 2000, with_dupes: int = 50) -> list[dict]:
    """Port of cell 17 customer_accounts block; injects DQ issues."""
    service_types = ["residential", "commercial", "industrial",
                     "RESIDENTIAL", "Commercial", "INDUSTRIAL"]
    rows = []
    for i in range(n):
        first = ctx.fake.first_name()
        last = ctx.fake.last_name()
        if ctx.rng.random() < 0.1:
            first = first.upper()
        elif ctx.rng.random() < 0.05:
            first = f"  {first}  "
        if ctx.rng.random() < 0.1:
            last = last.lower()
        addr = (None if ctx.rng.random() < 0.12
                else "" if ctx.rng.random() < 0.05
                else ctx.fake.street_address())
        zip_code = (None if ctx.rng.random() < 0.1
                    else str(ctx.rng.randint(1000, 9999)) if ctx.rng.random() < 0.05
                    else f"{ctx.rng.randint(100000, 999999)}" if ctx.rng.random() < 0.05
                    else ctx.fake.zipcode()[:5])
        service_type = None if ctx.rng.random() < 0.08 else ctx.rng.choice(service_types)
        if ctx.rng.random() < 0.1:
            created = (ctx.base_date + timedelta(days=ctx.rng.randint(-365, 0))).strftime("%d/%m/%Y")
        elif ctx.rng.random() < 0.05:
            created = "invalid-date"
        else:
            created = (ctx.base_date + timedelta(days=ctx.rng.randint(-365, 0))).strftime("%Y-%m-%dT%H:%M:%SZ")
        rows.append({
            "account_id": 10000 + i,
            "name": f"{first} {last}",
            "address": addr,
            "zip_code": zip_code,
            "service_type": service_type,
            "created_at": created,
        })
    for _ in range(with_dupes):
        dup = dict(ctx.rng.choice(rows))
        dup["created_at"] = (ctx.base_date + timedelta(days=ctx.rng.randint(-200, 0))).strftime("%Y-%m-%dT%H:%M:%SZ")
        rows.append(dup)
    return rows


# NOTE TO IMPLEMENTER: gen_meters, gen_weather, gen_events, gen_readings follow
# the same shape — port the corresponding blocks from the setup notebook
# (cells 17, sections "smart_meters", "weather_station", "grid_events",
# "meter_readings"). Each must:
#   1. Take (ctx, n, with_dupes, **fk_lookups) — fk_lookups carries
#      account_ids/meter_ids/station_coords as needed for referential integrity.
#   2. Return list[dict] with the same fields as the setup notebook.
#   3. Preserve the 5%-orphan pattern for meter→account and reading→meter FKs.
#   4. Use ctx.rng / ctx.fake exclusively (no module-level random).
def gen_meters(ctx: GeneratorContext, n: int, account_ids: list[int], with_dupes: int = 30) -> list[dict]:
    raise NotImplementedError("Port from setup notebook cell 17, smart_meters block")


def gen_weather(ctx: GeneratorContext, n_stations: int, n_readings: int, with_dupes: int = 20) -> tuple[list[dict], dict[int, dict]]:
    """Returns (rows, station_coords_map)."""
    raise NotImplementedError("Port from setup notebook cell 17, weather_station block")


def gen_events(ctx: GeneratorContext, n: int, with_dupes: int = 50) -> list[dict]:
    raise NotImplementedError("Port from setup notebook cell 17, grid_events block")


def gen_readings(ctx: GeneratorContext, n: int, meter_ids: list[int], with_dupes: int = 2000) -> list[dict]:
    raise NotImplementedError("Port from setup notebook cell 17, meter_readings block")
