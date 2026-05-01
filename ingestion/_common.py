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


def gen_meters(ctx: GeneratorContext, n: int, account_ids: list[int], with_dupes: int = 30) -> list[dict]:
    """Port of cell 17 smart_meters block; injects DQ issues incl. 5% orphan FKs."""
    meter_models = ["SM-2000", "SM-3000", "SM-4000", "SM-5000", "sm-2000", "SM_2000"]
    if not account_ids:
        raise ValueError("No account_ids provided! Cannot create smart_meters.")

    rows = []
    for i in range(n):
        meter_id = 50000 + i
        if ctx.rng.random() < 0.05:
            account_id = ctx.rng.randint(50000, 60000)  # orphaned FK
        else:
            account_id = ctx.rng.choice(account_ids)
        model = None if ctx.rng.random() < 0.08 else ctx.rng.choice(meter_models)
        if ctx.rng.random() < 0.1:
            installation_date = None
        elif ctx.rng.random() < 0.1:
            installation_date = (ctx.base_date + timedelta(days=ctx.rng.randint(-1000, 0))).strftime("%m-%d-%Y")
        else:
            installation_date = (ctx.base_date + timedelta(days=ctx.rng.randint(-1000, 0))).strftime("%Y-%m-%d")
        if ctx.rng.random() < 0.1:
            fw_version = None
        elif ctx.rng.random() < 0.1:
            fw_version = "v" + str(ctx.rng.randint(1, 5))
        elif ctx.rng.random() < 0.05:
            fw_version = "invalid"
        else:
            fw_version = f"v{ctx.rng.randint(1, 5)}.{ctx.rng.randint(0, 9)}.{ctx.rng.randint(0, 9)}"
        rows.append({
            "meter_id": meter_id,
            "account_id": account_id,
            "model": model,
            "installation_date": installation_date,
            "fw_version": fw_version,
        })
    for _ in range(with_dupes):
        rows.append(dict(ctx.rng.choice(rows)))
    return rows


def gen_weather(ctx: GeneratorContext, n_stations: int, n_readings: int, with_dupes: int = 20) -> tuple[list[dict], dict[int, dict[str, float]]]:
    """Port of cell 17 weather_station block.

    Returns (rows, station_coords_map). The station_coords map (station_id ->
    {"lat": float, "lon": float}) is exposed so downstream callers (e.g.
    gen_readings) can spatially correlate readings against fixed station
    locations.
    """
    station_coords: dict[int, dict[str, float]] = {}
    for i in range(n_stations):
        station_id = 20000 + i
        station_coords[station_id] = {
            "lat": round(float(ctx.fake.latitude()), 6),
            "lon": round(float(ctx.fake.longitude()), 6),
        }
    station_ids = list(station_coords.keys())

    rows = []
    for _ in range(n_readings):
        station_id = ctx.rng.choice(station_ids)
        base_coords = station_coords[station_id]
        if ctx.rng.random() < 0.1:
            lat = None
            lon = None
        elif ctx.rng.random() < 0.05:
            lat = round(ctx.rng.uniform(-100, 100), 6)
            lon = round(ctx.rng.uniform(-200, 200), 6)
        else:
            lat = round(base_coords["lat"] + ctx.rng.uniform(-0.1, 0.1), 6)
            lon = round(base_coords["lon"] + ctx.rng.uniform(-0.1, 0.1), 6)
        if ctx.rng.random() < 0.08:
            temp = None
        elif ctx.rng.random() < 0.05:
            temp = round(ctx.rng.uniform(-100, 200), 2)
        else:
            temp = round(ctx.rng.uniform(20, 100), 2)
        if ctx.rng.random() < 0.08:
            humidity = None
        elif ctx.rng.random() < 0.05:
            humidity = round(ctx.rng.uniform(-10, 150), 2)
        else:
            humidity = round(ctx.rng.uniform(20, 90), 2)
        if ctx.rng.random() < 0.08:
            wind_speed = None
        elif ctx.rng.random() < 0.05:
            wind_speed = round(ctx.rng.uniform(-5, 0), 2)
        else:
            wind_speed = round(ctx.rng.uniform(0, 50), 2)
        if ctx.rng.random() < 0.1:
            recorded_at = (ctx.base_date + timedelta(days=ctx.rng.randint(0, 180), hours=ctx.rng.randint(0, 23))).strftime("%d/%m/%Y %H:%M")
        elif ctx.rng.random() < 0.05:
            recorded_at = "invalid-timestamp"
        else:
            recorded_at = (ctx.base_date + timedelta(days=ctx.rng.randint(0, 180), hours=ctx.rng.randint(0, 23))).strftime("%Y-%m-%dT%H:%M:%SZ")
        rows.append({
            "station_id": station_id,
            "temp": temp,
            "humidity": humidity,
            "wind_speed": wind_speed,
            "recorded_at": recorded_at,
            "lat": lat,
            "lon": lon,
        })
    for _ in range(with_dupes):
        rows.append(dict(ctx.rng.choice(rows)))
    return rows, station_coords


def gen_events(ctx: GeneratorContext, n: int, with_dupes: int = 50) -> list[dict]:
    """Port of cell 17 grid_events block; standalone (no FKs)."""
    event_types = [
        "voltage_spike", "voltage_drop", "power_outage", "maintenance",
        "fault_detected", "VOLTAGE_SPIKE", "unknown",
    ]
    severities = ["low", "medium", "high", "critical", "LOW", "High", None]

    rows = []
    for i in range(n):
        event_id = 90000 + i
        if ctx.rng.random() < 0.1:
            timestamp = (ctx.base_date + timedelta(
                days=ctx.rng.randint(0, 180),
                hours=ctx.rng.randint(0, 23),
                minutes=ctx.rng.randint(0, 59),
            )).strftime("%m/%d/%Y %H:%M:%S")
        elif ctx.rng.random() < 0.05:
            timestamp = "invalid"
        else:
            timestamp = (ctx.base_date + timedelta(
                days=ctx.rng.randint(0, 180),
                hours=ctx.rng.randint(0, 23),
                minutes=ctx.rng.randint(0, 59),
            )).strftime("%Y-%m-%dT%H:%M:%SZ")
        event_type = None if ctx.rng.random() < 0.08 else ctx.rng.choice(event_types)
        severity = ctx.rng.choice(severities)
        if ctx.rng.random() < 0.1:
            description_json = None
        elif ctx.rng.random() < 0.1:
            description_json = "not a json"
        elif ctx.rng.random() < 0.05:
            description_json = '{"message": "incomplete'
        elif ctx.rng.random() < 0.05:
            description_json = json.dumps({
                "message": "valid",
                "code": ctx.rng.randint(100, 999),
                "extra_field": "data",
            })
        else:
            description_json = json.dumps({
                "message": f"Event {event_id} occurred",
                "code": ctx.rng.randint(100, 999),
                "location": ctx.fake.city(),
                "zone": ctx.rng.randint(1, 10),
            })
        rows.append({
            "event_id": event_id,
            "timestamp": timestamp,
            "event_type": event_type,
            "severity": severity,
            "description_json": description_json,
        })
    for _ in range(with_dupes):
        rows.append(dict(ctx.rng.choice(rows)))
    return rows


def gen_readings(ctx: GeneratorContext, n: int, meter_ids: list[int], with_dupes: int = 2000) -> list[dict]:
    """Port of cell 17 meter_readings block; preserves 5% orphan FK pattern."""
    status_codes = ["active", "inactive", "error", "maintenance", "ACTIVE", "Error", None]
    if not meter_ids:
        raise ValueError("No meter_ids provided! Cannot create meter_readings.")

    rows = []
    for i in range(n):
        reading_id = 100000 + i
        if ctx.rng.random() < 0.05:
            meter_id = ctx.rng.randint(70000, 80000)  # orphaned FK
        else:
            meter_id = ctx.rng.choice(meter_ids)
        if ctx.rng.random() < 0.08:
            kwh = None
        elif ctx.rng.random() < 0.05:
            kwh = round(ctx.rng.uniform(-10, 0), 3)
        elif ctx.rng.random() < 0.03:
            kwh = round(ctx.rng.uniform(10000, 50000), 3)
        else:
            kwh = round(ctx.rng.uniform(0.1, 50.0), 3)
        if ctx.rng.random() < 0.08:
            voltage = None
        elif ctx.rng.random() < 0.05:
            voltage = round(ctx.rng.uniform(0, 50), 2)
        elif ctx.rng.random() < 0.05:
            voltage = round(ctx.rng.uniform(300, 500), 2)
        else:
            voltage = round(ctx.rng.uniform(110, 240), 2)
        if ctx.rng.random() < 0.1:
            timestamp = (ctx.base_date + timedelta(
                days=ctx.rng.randint(0, 180),
                hours=ctx.rng.randint(0, 23),
                minutes=ctx.rng.randint(0, 59),
            )).strftime("%d/%m/%Y %H:%M:%S")
        elif ctx.rng.random() < 0.05:
            timestamp = "invalid-timestamp"
        elif ctx.rng.random() < 0.02:
            timestamp = None
        else:
            timestamp = (ctx.base_date + timedelta(
                days=ctx.rng.randint(0, 180),
                hours=ctx.rng.randint(0, 23),
                minutes=ctx.rng.randint(0, 59),
            )).strftime("%Y-%m-%dT%H:%M:%SZ")
        status_code = ctx.rng.choice(status_codes)
        rows.append({
            "id": reading_id,
            "meter_id": meter_id,
            "kwh": kwh,
            "voltage": voltage,
            "timestamp": timestamp,
            "status_code": status_code,
        })
    for _ in range(with_dupes):
        rows.append(dict(ctx.rng.choice(rows)))
    return rows
