#!/usr/bin/env python3
"""Read latency benchmarks.

Run: python benchmarks/bench_read.py
Prerequisite: run bench_write.py first to populate the warehouse.
"""
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyiceberg.catalog import load_catalog
from schema import ICEBERG_SCHEMA, PARTITION_SPEC
from snapshot_id import make_snapshot_id
from synthetic import generate_records, generate_records_iter
from writer import write_community_snapshot, write_snapshot
from sidecar import ECRegistrationRecord, MeteringPointRecord
from reader import (
    query_community_billing,
    query_snap_nonsnap,
    query_registration_detail,
    query_cross_community,
)

CATALOG_URI = "postgresql+psycopg2://iceberg:iceberg@localhost:5432/iceberg_test"
PERIOD      = datetime(2026, 1, 1, tzinfo=timezone.utc)
PERIOD_END  = datetime(2026, 2, 1, tzinfo=timezone.utc)
EXPORTED_AT = datetime(2026, 3, 26, 12, 0, 0, tzinfo=timezone.utc)
WAREHOUSE   = "/tmp/iceberg_bench_warehouse"
SIDECAR_DIR = f"{WAREHOUSE}/sidecars"


def setup_and_write(num_points: int = 20, community_id: int = 1):
    cat = load_catalog("bench", **{
        "type":      "sql",
        "uri":       CATALOG_URI,
        "warehouse": f"file://{WAREHOUSE}",
    })
    try:
        cat.create_namespace("bench")
    except Exception:
        pass
    ident = f"bench.records_{uuid.uuid4().hex[:8]}"
    table = cat.create_table(ident, schema=ICEBERG_SCHEMA, partition_spec=PARTITION_SPEC)

    records = generate_records(community_id=community_id, period_start=PERIOD, num_points=num_points, seed=42)
    sid     = make_snapshot_id(community_id, PERIOD, 1)
    uri, _ = write_community_snapshot(table, records, snapshot_id=sid, exported_at=EXPORTED_AT)
    return uri, sid, records[0]["ec_registration_id"]


def setup_and_write_200pt():
    """Write a 200-point outlier community for read benchmarks."""
    import os
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
    table = cat.create_table(
        f"bench.read200_{uuid.uuid4().hex[:8]}",
        schema=ICEBERG_SCHEMA,
        partition_spec=PARTITION_SPEC,
    )
    records = generate_records_iter(community_id=200, period_start=PERIOD, num_points=200, seed=200)
    sid     = make_snapshot_id(200, PERIOD, 1)
    write_snapshot(
        table, records,
        snapshot_id=sid,
        exported_at=EXPORTED_AT,
        community_id=200,
        community_name="Outlier Community",
        community_ec_id="AT-EC-000200",
        period_start=PERIOD,
        period_end=PERIOD_END,
        ec_registrations=[],
        metering_points=[],
        sidecar_dir=SIDECAR_DIR,
    )
    return table.metadata_location, sid


def bench_query(label: str, fn, *args, iterations: int = 5):
    times = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        fn(*args)
        times.append(time.perf_counter() - t0)
    avg = sum(times) / len(times)
    mn  = min(times)
    mx  = max(times)
    print(f"[{label}] avg={avg:.3f}s  min={mn:.3f}s  max={mx:.3f}s  ({iterations} runs)")


if __name__ == "__main__":
    print("=== Read Benchmarks ===\n")
    print("Setting up test data (20-point community)...")
    uri, sid, reg_id = setup_and_write(num_points=20, community_id=1)

    print(f"\nMetadata URI: {uri}\n")

    bench_query("community_billing ", query_community_billing,      uri, 1, sid)
    bench_query("snap_nonsnap      ", query_snap_nonsnap,           uri, 1, sid)
    bench_query("registration_detail", query_registration_detail,   uri, 1, sid, reg_id)

    print("\nSetting up cross-community data (3 communities)...")
    uris_and_sids = []
    for cid in [10, 11, 12]:
        u, s, _ = setup_and_write(num_points=20, community_id=cid)
        uris_and_sids.append((u, s))

    last_uri = uris_and_sids[-1][0]
    all_sids = [s for _, s in uris_and_sids]
    bench_query("cross_community   ", query_cross_community, last_uri, all_sids)

    print("\n=== Read Benchmarks: 200-point outlier community ===\n")
    print("Setting up 200-point community (this may take ~10s)...")
    uri200, sid200 = setup_and_write_200pt()
    print(f"Metadata URI: {uri200}\n")

    bench_query("[community_billing 200pt]", lambda: query_community_billing(uri200, 200, sid200))
    bench_query("[snap_nonsnap 200pt     ]", lambda: query_snap_nonsnap(uri200, 200, sid200))
