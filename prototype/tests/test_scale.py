# prototype/tests/test_scale.py
"""
Phase 2 scale tests. All marked @pytest.mark.slow.
Run with: pytest -m slow tests/test_scale.py -v

These tests use the session-scoped scale_table_avg (20pts) and
scale_table_outlier (200pts) fixtures from conftest.py to avoid
rewriting large tables per test.
"""
import os
import time
from decimal import Decimal

import duckdb
import pytest

from reader import (
    query_community_billing,
    query_snap_nonsnap,
    query_registration_detail,
)
from snapshot_id import make_snapshot_id
from synthetic import generate_records
from writer import write_community_snapshot
from datetime import datetime, timezone

PERIOD   = datetime(2026, 1, 1, tzinfo=timezone.utc)
EXPORTED = datetime(2026, 3, 26, 12, 0, 0, tzinfo=timezone.utc)


# ── Row count correctness ──────────────────────────────────────────────────────

@pytest.mark.slow
@pytest.mark.parametrize("fixture_name,expected_records", [
    ("scale_table_avg",     297_600),   # 20pts × 96 slots/day × 31 days × 5 types
    ("scale_table_outlier", 2_976_000), # 200pts × 96 × 31 × 5
])
def test_row_count_matches_at_scale(fixture_name, expected_records, request):
    data = request.getfixturevalue(fixture_name)
    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    count = conn.execute(
        f"SELECT COUNT(*) FROM iceberg_scan('{data['uri']}') "
        f"WHERE community_id = {data['community_id']} AND snapshot_id = {data['sid']}"
    ).fetchone()[0]
    assert count == expected_records


# ── Write throughput ───────────────────────────────────────────────────────────

@pytest.mark.slow
@pytest.mark.parametrize("fixture_name,max_seconds", [
    ("scale_table_avg",     30),   # 297k records: fail if > 30s
    ("scale_table_outlier", 120),  # 3M records:   fail if > 120s
])
def test_write_completed_within_time_limit(fixture_name, max_seconds, request):
    """
    Indirect timing test: the session fixture already wrote the data.
    Here we time a fresh write to a new table to get a clean measurement.
    """
    from schema import ICEBERG_SCHEMA, PARTITION_SPEC
    import uuid

    data    = request.getfixturevalue(fixture_name)
    catalog = request.getfixturevalue("catalog")
    ident   = f"billing_test.timing_{uuid.uuid4().hex[:8]}"
    table   = catalog.create_table(ident, schema=ICEBERG_SCHEMA, partition_spec=PARTITION_SPEC)

    sid = make_snapshot_id(data["community_id"] + 1000, data["period"], 1)

    t0      = time.perf_counter()
    write_community_snapshot(table, data["records"], snapshot_id=sid, exported_at=EXPORTED)
    elapsed = time.perf_counter() - t0

    catalog.drop_table(ident)

    rps = len(data["records"]) / elapsed
    print(f"\n[{fixture_name}] {len(data['records']):,} records in {elapsed:.1f}s = {rps:,.0f} rec/s")
    assert elapsed < max_seconds, (
        f"Write took {elapsed:.1f}s, limit is {max_seconds}s for {data['num_points']} points"
    )


# ── Read latency ───────────────────────────────────────────────────────────────

@pytest.mark.slow
@pytest.mark.parametrize("fixture_name,max_ms", [
    ("scale_table_avg",     1000),  # 297k rows: target sub-second
    ("scale_table_outlier", 2000),  # 3M rows:   target under 2 seconds
])
def test_billing_query_latency_at_scale(fixture_name, max_ms, request):
    data = request.getfixturevalue(fixture_name)

    # Warm up DuckDB iceberg extension (first call loads metadata)
    query_community_billing(data["uri"], data["community_id"], data["sid"])

    t0     = time.perf_counter()
    result = query_community_billing(data["uri"], data["community_id"], data["sid"])
    elapsed_ms = (time.perf_counter() - t0) * 1000

    print(f"\n[{fixture_name}] {len(result):,} rows in {elapsed_ms:.0f}ms")
    assert elapsed_ms < max_ms, f"Query took {elapsed_ms:.0f}ms, limit is {max_ms}ms"


@pytest.mark.slow
@pytest.mark.parametrize("fixture_name,max_ms", [
    ("scale_table_avg",     1500),
    ("scale_table_outlier", 3000),
])
def test_snap_nonsnap_latency_at_scale(fixture_name, max_ms, request):
    data = request.getfixturevalue(fixture_name)

    # Warm up
    query_snap_nonsnap(data["uri"], data["community_id"], data["sid"])

    t0         = time.perf_counter()
    result     = query_snap_nonsnap(data["uri"], data["community_id"], data["sid"])
    elapsed_ms = (time.perf_counter() - t0) * 1000

    print(f"\n[{fixture_name}] {len(result):,} groups in {elapsed_ms:.0f}ms")
    assert elapsed_ms < max_ms, f"SNAP/NON-SNAP query took {elapsed_ms:.0f}ms, limit is {max_ms}ms"


@pytest.mark.slow
def test_registration_detail_latency_at_scale(scale_table_avg):
    """Per-registration detail query must be sub-second even for a 20-point community."""
    data = scale_table_avg
    # First registration ID from synthetic.py: community_id * 1000 + 0
    reg_id = data["community_id"] * 1000

    # Warm up
    query_registration_detail(data["uri"], data["community_id"], data["sid"], reg_id)

    t0         = time.perf_counter()
    result     = query_registration_detail(data["uri"], data["community_id"], data["sid"], reg_id)
    elapsed_ms = (time.perf_counter() - t0) * 1000

    # 1 point × 14,880 records per registration
    assert len(result) == 14_880
    print(f"\n[reg-detail-avg] {len(result):,} rows in {elapsed_ms:.0f}ms")
    assert elapsed_ms < 1000, f"Registration detail query took {elapsed_ms:.0f}ms"


# ── File sizes ─────────────────────────────────────────────────────────────────

@pytest.mark.slow
@pytest.mark.parametrize("fixture_name,expected_kb,tolerance_factor", [
    ("scale_table_avg",     3500,  1.5),  # synthetic baseline ~3164 KB; production compresses ~11× better
    ("scale_table_outlier", 27000, 1.5),  # synthetic baseline ~25415 KB; linear with point count
])
def test_parquet_file_size_within_spec(fixture_name, expected_kb, tolerance_factor, request, warehouse_dir):
    """Parquet file sizes must not exceed baseline × 1.5 (regression guard).

    Spec §6.5 predictions apply to production data. Synthetic random decimals
    compress ~11× worse than real meter readings, so baselines here reflect
    actual synthetic measurements. A file > 1.5× baseline signals an unintended
    schema change or compression misconfiguration.
    """
    data = request.getfixturevalue(fixture_name)
    cid  = data["community_id"]

    parquet_files = []
    for root, dirs, files in os.walk(warehouse_dir):
        if f"community_id={cid}" in root:
            for f in files:
                if f.endswith(".parquet"):
                    parquet_files.append(os.path.join(root, f))

    assert parquet_files, (
        f"No parquet files found for community_id={cid} under {warehouse_dir}. "
        "Check that the warehouse_dir fixture points to the right location."
    )

    total_kb = sum(os.path.getsize(f) for f in parquet_files) / 1024
    print(f"\n[{fixture_name}] parquet={total_kb:.0f}KB | spec≈{expected_kb}KB | {len(parquet_files)} file(s)")

    assert total_kb < expected_kb * tolerance_factor, (
        f"Parquet files are {total_kb:.0f}KB, expected ≤{expected_kb * tolerance_factor:.0f}KB "
        f"(spec prediction: ~{expected_kb}KB × {tolerance_factor}× tolerance)"
    )


# ── 100-community batch write ──────────────────────────────────────────────────

@pytest.mark.slow
def test_100_community_batch_write(catalog, warehouse_dir):
    """Write 100 communities sequentially; spot-check 10 via DuckDB."""
    import uuid
    import random
    from schema import ICEBERG_SCHEMA, PARTITION_SPEC

    ident = f"billing_test.batch100_{uuid.uuid4().hex[:8]}"
    table = catalog.create_table(ident, schema=ICEBERG_SCHEMA, partition_spec=PARTITION_SPEC)

    written = []
    for cid in range(1, 101):
        records     = generate_records(community_id=cid, period_start=PERIOD, num_points=1, seed=cid)
        sid         = make_snapshot_id(cid, PERIOD, 1)
        uri, _      = write_community_snapshot(table, records, snapshot_id=sid, exported_at=EXPORTED)
        written.append((cid, sid, uri, len(records)))

    # Spot-check 10 random communities
    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    rng = random.Random(99)
    last_uri = written[-1][2]  # latest metadata covers all data

    for cid, sid, _, expected_count in rng.sample(written, 10):
        count = conn.execute(
            f"SELECT COUNT(*) FROM iceberg_scan('{last_uri}') "
            f"WHERE community_id = {cid} AND snapshot_id = {sid}"
        ).fetchone()[0]
        assert count == expected_count, f"community {cid}: expected {expected_count} rows, got {count}"

    catalog.drop_table(ident)


# ── Version accumulation degradation ──────────────────────────────────────────

@pytest.mark.slow
def test_version_accumulation_stays_sub_second(catalog):
    """
    Write 5 versions for the same community/period.
    Read latency for v1 must stay under 1 second even with v2–v5 in the same partition.

    This validates spec §8.6: read latency grows linearly with versions but stays
    sub-second for up to 5 versions at average community size.
    """
    import uuid
    from schema import ICEBERG_SCHEMA, PARTITION_SPEC

    ident = f"billing_test.versions_{uuid.uuid4().hex[:8]}"
    table = catalog.create_table(ident, schema=ICEBERG_SCHEMA, partition_spec=PARTITION_SPEC)
    last_uri = None

    for v in range(1, 6):
        records      = generate_records(community_id=300, period_start=PERIOD, num_points=20, seed=v * 100)
        sid          = make_snapshot_id(300, PERIOD, v)
        last_uri, _  = write_community_snapshot(table, records, snapshot_id=sid, exported_at=EXPORTED)

    # Query each version using the final metadata URI (which covers all 5 versions)
    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")

    latencies = {}
    for v in range(1, 6):
        sid = make_snapshot_id(300, PERIOD, v)
        # Warm up
        conn.execute(
            f"SELECT COUNT(*) FROM iceberg_scan('{last_uri}') "
            f"WHERE community_id = 300 AND snapshot_id = {sid}"
        ).fetchone()
        t0         = time.perf_counter()
        count      = conn.execute(
            f"SELECT COUNT(*) FROM iceberg_scan('{last_uri}') "
            f"WHERE community_id = 300 AND snapshot_id = {sid}"
        ).fetchone()[0]
        elapsed_ms = (time.perf_counter() - t0) * 1000
        latencies[v] = elapsed_ms
        assert count == 297_600, f"version {v}: expected 297,600 rows, got {count}"

    print("\n[version accumulation latencies]")
    for v, ms in latencies.items():
        print(f"  v{v}: {ms:.0f}ms")

    # All versions must be queryable in under 1 second even when 5 versions coexist
    slow_versions = {v: ms for v, ms in latencies.items() if ms >= 1000}
    assert not slow_versions, (
        f"Version accumulation caused slow queries: {slow_versions}. "
        "Compaction may be needed sooner than expected."
    )

    catalog.drop_table(ident)
