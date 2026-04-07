#!/usr/bin/env python3
"""Memory profiling benchmarks.

Write path: memray run -o /tmp/write_Npts.bin benchmarks/bench_memory.py write N
Read path:  python benchmarks/bench_memory.py read N  (OS RSS delta, no memray)

Phase 2 success thresholds (from spec §11):
- Write: bounded memory delta ≤ ~300MB for 200-point community
- Read: sub-second latency for single-community queries
"""
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyiceberg.catalog import load_catalog
from schema import ICEBERG_SCHEMA, PARTITION_SPEC
from snapshot_id import make_snapshot_id
from synthetic import generate_records_iter
from writer import write_snapshot
from reader import query_community_billing
from sidecar import ECRegistrationRecord, MeteringPointRecord

CATALOG_URI = "postgresql+psycopg2://iceberg:iceberg@localhost:5432/iceberg_test"
PERIOD      = datetime(2026, 1, 1, tzinfo=timezone.utc)
PERIOD_END  = datetime(2026, 2, 1, tzinfo=timezone.utc)
EXPORTED_AT = datetime(2026, 3, 26, 12, 0, 0, tzinfo=timezone.utc)
WAREHOUSE   = "/tmp/iceberg_bench_warehouse"
SIDECAR_DIR = f"{WAREHOUSE}/sidecars"


def _rss_mb() -> float:
    """Return current process RSS in MB (Linux only)."""
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024
    return 0.0


def bench_write_memory(num_points: int):
    cat = load_catalog("bench", **{
        "type":      "sql",
        "uri":       CATALOG_URI,
        "warehouse": f"file://{WAREHOUSE}",
    })
    try:
        cat.create_namespace("bench")
    except Exception:
        pass
    os.makedirs(SIDECAR_DIR, exist_ok=True)
    ident = f"bench.mem_{uuid.uuid4().hex[:8]}"
    table = cat.create_table(ident, schema=ICEBERG_SCHEMA, partition_spec=PARTITION_SPEC)

    records = generate_records_iter(community_id=1, period_start=PERIOD, num_points=num_points, seed=42)
    sid     = make_snapshot_id(1, PERIOD, 1)

    rss_before = _rss_mb()
    meta = write_snapshot(
        table, records,
        snapshot_id=sid,
        exported_at=EXPORTED_AT,
        community_id=1,
        community_name="Community 1",
        community_ec_id="AT-EC-000001",
        period_start=PERIOD,
        period_end=PERIOD_END,
        ec_registrations=[
            ECRegistrationRecord(
                id=1, egon_id=10, meteringpoint_id=100, community_id=1,
                registered_from=PERIOD, registered_until=None,
            )
        ],
        metering_points=[
            MeteringPointRecord(id=100, egon_id=1000, name="MP-1", energy_direction=1)
        ],
        sidecar_dir=SIDECAR_DIR,
    )
    rss_after = _rss_mb()

    delta = rss_after - rss_before
    print(f"Write ({num_points} pts, {meta.record_count:,} records): RSS delta = {delta:+.1f} MB")
    print(f"  Before: {rss_before:.1f} MB  After: {rss_after:.1f} MB")
    return meta.iceberg_metadata_uri, sid


def bench_read_memory(uri: str, sid: int):
    rss_before = _rss_mb()
    query_community_billing(uri, community_id=1, snapshot_id=sid)
    rss_after = _rss_mb()

    delta = rss_after - rss_before
    print(f"Read  (community_billing): RSS delta = {delta:+.1f} MB")


if __name__ == "__main__":
    mode    = sys.argv[1] if len(sys.argv) > 1 else "write"
    num_pts = int(sys.argv[2]) if len(sys.argv) > 2 else 20

    print(f"=== Memory Benchmark: {mode}, {num_pts} points ===\n")

    if mode == "write":
        uri, sid = bench_write_memory(num_pts)
        print("\n(For memray profiling: memray run -o /tmp/write.bin benchmarks/bench_memory.py write N)")

    elif mode == "read":
        uri, sid = bench_write_memory(num_pts)
        print()
        bench_read_memory(uri, sid)

    else:
        print(f"Unknown mode: {mode}. Use 'write' or 'read'.")
        sys.exit(1)
