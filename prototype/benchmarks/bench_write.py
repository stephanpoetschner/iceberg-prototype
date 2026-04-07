#!/usr/bin/env python3
"""Write throughput benchmarks.

Run: python benchmarks/bench_write.py
"""
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pyiceberg.catalog import load_catalog
from schema import ICEBERG_SCHEMA, PARTITION_SPEC
from snapshot_id import make_snapshot_id
from synthetic import generate_records_iter, CommunityProfile
from writer import write_snapshot
from sidecar import ECRegistrationRecord, MeteringPointRecord

CATALOG_URI  = "postgresql+psycopg2://iceberg:iceberg@localhost:5432/iceberg_test"
PERIOD       = datetime(2026, 1, 1, tzinfo=timezone.utc)
PERIOD_END   = datetime(2026, 2, 1, tzinfo=timezone.utc)
EXPORTED_AT  = datetime(2026, 3, 26, 12, 0, 0, tzinfo=timezone.utc)
WAREHOUSE    = "/tmp/iceberg_bench_warehouse"
SIDECAR_DIR  = f"{WAREHOUSE}/sidecars"


def _synthetic_context(community_id: int) -> dict:
    """Minimal synthetic business context for write_snapshot()."""
    return dict(
        community_name=f"Community {community_id}",
        community_ec_id=f"AT-EC-{community_id:06d}",
        period_start=PERIOD,
        period_end=PERIOD_END,
        ec_registrations=[
            ECRegistrationRecord(
                id=community_id,
                egon_id=community_id * 10,
                meteringpoint_id=community_id * 100,
                community_id=community_id,
                registered_from=PERIOD,
                registered_until=None,
            )
        ],
        metering_points=[
            MeteringPointRecord(
                id=community_id * 100,
                egon_id=community_id * 1000,
                name=f"MP-{community_id}",
                energy_direction=1,
            )
        ],
    )


def setup_table(namespace: str = "bench"):
    cat = load_catalog("bench", **{
        "type":      "sql",
        "uri":       CATALOG_URI,
        "warehouse": f"file://{WAREHOUSE}",
    })
    try:
        cat.create_namespace(namespace)
    except Exception:
        pass
    os.makedirs(SIDECAR_DIR, exist_ok=True)
    ident = f"{namespace}.records_{uuid.uuid4().hex[:8]}"
    table = cat.create_table(ident, schema=ICEBERG_SCHEMA, partition_spec=PARTITION_SPEC)
    return table


def bench_single_community(num_points: int, label: str):
    table   = setup_table()
    records = generate_records_iter(community_id=1, period_start=PERIOD, num_points=num_points, seed=42)
    sid     = make_snapshot_id(1, PERIOD, 1)

    t0   = time.perf_counter()
    meta = write_snapshot(
        table, records,
        snapshot_id=sid,
        exported_at=EXPORTED_AT,
        community_id=1,
        sidecar_dir=SIDECAR_DIR,
        **_synthetic_context(1),
    )
    elapsed = time.perf_counter() - t0

    rps = meta.record_count / elapsed
    print(f"[{label}] {meta.record_count:>10,} records | {elapsed:6.2f}s | {rps:>12,.0f} rec/s | {num_points} points")
    return elapsed, meta.record_count, meta.iceberg_metadata_uri


def bench_batch_write(num_communities: int, num_points: int, label: str):
    table  = setup_table()
    t0     = time.perf_counter()
    total  = 0
    for cid in range(1, num_communities + 1):
        records = generate_records_iter(community_id=cid, period_start=PERIOD, num_points=num_points, seed=cid)
        sid     = make_snapshot_id(cid, PERIOD, 1)
        meta    = write_snapshot(
            table, records,
            snapshot_id=sid,
            exported_at=EXPORTED_AT,
            community_id=cid,
            sidecar_dir=SIDECAR_DIR,
            **_synthetic_context(cid),
        )
        total += meta.record_count
        if cid % 100 == 0 or cid == num_communities:
            elapsed = time.perf_counter() - t0
            rps = total / elapsed
            print(f"  [{label}] community {cid}/{num_communities} | {total:>12,} records | {elapsed:7.1f}s | {rps:>10,.0f} rec/s", flush=True)

    elapsed = time.perf_counter() - t0
    rps     = total / elapsed
    print(f"[{label}] TOTAL {total:>12,} records | {elapsed:7.1f}s | {rps:>12,.0f} rec/s | {num_communities} communities")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", action="store_true", help="Run the full 2500-community benchmark (~30 min)")
    args = parser.parse_args()

    print("=== Write Benchmarks ===\n")
    bench_single_community(num_points=1,   label="single-1pt  ")
    bench_single_community(num_points=20,  label="single-avg  ")
    bench_single_community(num_points=200, label="outlier-200pt")
    print()
    bench_batch_write(num_communities=10,  num_points=20, label="batch-10  ")
    bench_batch_write(num_communities=100, num_points=20, label="batch-100 ")
    if args.scale:
        print("\n=== Full-Scale: 2500 communities ===")
        bench_batch_write(num_communities=2500, num_points=20, label="scale-2500")
