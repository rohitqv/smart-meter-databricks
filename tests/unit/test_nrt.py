# tests/unit/test_nrt.py
from datetime import datetime, timezone
from unittest.mock import patch

from ingestion.nrt.simulate_nrt_feed import generate_tick


def test_generate_tick_produces_one_file_per_source():
    written = []

    def fake_write(records, bucket, key):
        written.append(key)
        return len(list(records))

    with patch("ingestion.nrt.simulate_nrt_feed.write_jsonl_to_s3", side_effect=fake_write):
        ts = datetime(2026, 5, 1, 14, 30, 45, tzinfo=timezone.utc)
        generate_tick(bucket="test-bucket", ts=ts, rows_per_table=10, seed=1)

    assert len(written) == 5
    assert all(k.startswith("raw/nrt/") for k in written)
    assert all("dt=2026-05-01" in k for k in written)
    assert all("20260501T143045Z" in k for k in written)


def test_generate_tick_uses_now_utc_when_ts_omitted():
    with patch("ingestion.nrt.simulate_nrt_feed.write_jsonl_to_s3", return_value=10) as w:
        generate_tick(bucket="test-bucket", rows_per_table=5, seed=1)
    written_keys = [call.args[2] for call in w.call_args_list]
    # Format check: timestamp segment is exactly 16 chars (YYYYMMDDTHHMMSSZ)
    for key in written_keys:
        ts_segment = key.split("_")[-2]
        assert len(ts_segment) == 16
        assert ts_segment.endswith("Z")
