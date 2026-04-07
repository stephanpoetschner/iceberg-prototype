# prototype/tests/test_advanced.py
"""
Advanced correctness tests:
- Null aggregation correctness (SUM skips nulls, COUNT(*) includes them)
- Concurrent writes (two processes, different communities, same table)
"""
import multiprocessing
import sys
import threading
from datetime import datetime, timezone
from decimal import Decimal

import duckdb
import pytest

from lock import SnapshotLockError
from snapshot_id import make_snapshot_id, SnapshotAlreadyExistsError
from synthetic import generate_records
from writer import write_community_snapshot

PERIOD   = datetime(2026, 1, 1, tzinfo=timezone.utc)
EXPORTED = datetime(2026, 3, 26, 12, 0, 0, tzinfo=timezone.utc)


def test_null_aggregation_sum_skips_nulls(iceberg_table):
    """SUM() skips NULLs; COUNT(*) includes null rows; COUNT(value) excludes them."""
    from schema import ARROW_SCHEMA
    import pyarrow as pa

    slot_time   = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    snapshot_id = make_snapshot_id(50, PERIOD, 1)

    records = [
        {
            "community_id": 50, "time": slot_time, "record_type": 1,
            "ec_registration_id": 50000, "metering_point_id": 500000,
            "value": Decimal("1.000000"), "value_type": 1, "message_created_at": EXPORTED,
        },
        {
            "community_id": 50, "time": slot_time, "record_type": 2,
            "ec_registration_id": 50000, "metering_point_id": 500000,
            "value": Decimal("2.000000"), "value_type": 1, "message_created_at": EXPORTED,
        },
        {
            "community_id": 50, "time": slot_time, "record_type": 3,
            "ec_registration_id": 50000, "metering_point_id": 500000,
            "value": None, "value_type": 1, "message_created_at": EXPORTED,
        },
    ]

    uri, _ = write_community_snapshot(iceberg_table, records, snapshot_id=snapshot_id, exported_at=EXPORTED)

    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    base = f"FROM iceberg_scan('{uri}') WHERE snapshot_id = {snapshot_id}"

    total_sum   = conn.execute(f"SELECT SUM(value) {base}").fetchone()[0]
    count_star  = conn.execute(f"SELECT COUNT(*) {base}").fetchone()[0]
    count_value = conn.execute(f"SELECT COUNT(value) {base}").fetchone()[0]

    assert total_sum   == Decimal("3.000000"), f"SUM should skip NULL and return 1+2=3, got {total_sum}"
    assert count_star  == 3,                   f"COUNT(*) includes null rows, got {count_star}"
    assert count_value == 2,                   f"COUNT(value) excludes null rows, got {count_value}"


def _worker_write(table_ident: str, community_id: int, warehouse_dir: str, result_queue):
    """Subprocess worker: create catalog connection, write one community."""
    import time

    try:
        from pyiceberg.catalog import load_catalog
        from snapshot_id import make_snapshot_id
        from synthetic import generate_records
        from writer import write_community_snapshot
        from datetime import datetime, timezone

        CATALOG_URI = "postgresql+psycopg2://iceberg:iceberg@localhost:5432/iceberg_test"
        period      = datetime(2026, 1, 1, tzinfo=timezone.utc)
        exported_at = datetime(2026, 3, 26, 12, 0, 0, tzinfo=timezone.utc)

        cat   = load_catalog("test", **{"type": "sql", "uri": CATALOG_URI, "warehouse": f"file://{warehouse_dir}"})

        records     = generate_records(community_id=community_id, period_start=period, num_points=1, seed=community_id)
        snapshot_id = make_snapshot_id(community_id, period, 1)

        # Retry up to 5 times on optimistic concurrency conflicts (SQL catalog OCC).
        last_err = None
        for attempt in range(5):
            try:
                table = cat.load_table(table_ident)
                uri, count = write_community_snapshot(table, records, snapshot_id=snapshot_id, exported_at=exported_at)
                result_queue.put({"ok": True, "uri": uri, "community_id": community_id, "count": count, "snapshot_id": snapshot_id})
                return
            except Exception as e:
                last_err = e
                if "updated by another process" in str(e):
                    time.sleep(0.1 * (attempt + 1))
                    continue
                raise

        raise last_err
    except Exception as e:
        result_queue.put({"ok": False, "error": str(e), "community_id": community_id})


def test_concurrent_writes_different_communities(iceberg_table, catalog, warehouse_dir):
    """Two workers write different communities simultaneously. Both must succeed."""
    table_ident = f"{iceberg_table._identifier[0]}.{iceberg_table._identifier[1]}"

    q = multiprocessing.Queue()
    p1 = multiprocessing.Process(target=_worker_write, args=(table_ident, 60, warehouse_dir, q))
    p2 = multiprocessing.Process(target=_worker_write, args=(table_ident, 61, warehouse_dir, q))

    p1.start(); p2.start()
    p1.join(timeout=120); p2.join(timeout=120)

    if p1.is_alive() or p2.is_alive():
        p1.terminate(); p2.terminate()
        pytest.fail("Worker process(es) timed out after 120s")

    results = [q.get_nowait() for _ in range(2)]
    for r in results:
        assert r["ok"], f"Worker for community {r['community_id']} failed: {r.get('error')}"

    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")

    # Refresh the table object to get the latest metadata location after both subprocess writes
    table_ident = f"{iceberg_table._identifier[0]}.{iceberg_table._identifier[1]}"
    refreshed_table = catalog.load_table(table_ident)
    latest_uri = refreshed_table.metadata_location

    for r in results:
        count = conn.execute(
            f"SELECT COUNT(*) FROM iceberg_scan('{latest_uri}') "
            f"WHERE community_id = {r['community_id']} AND snapshot_id = {r['snapshot_id']}"
        ).fetchone()[0]
        assert count == r["count"], f"community {r['community_id']}: expected {r['count']} rows, got {count}"


def test_redis_lock_prevents_concurrent_duplicate_writes(iceberg_table, redis_client):
    """Two threads writing the same snapshot_id: exactly one must succeed."""
    records = generate_records(community_id=90, period_start=PERIOD, num_points=1, seed=90)
    sid = make_snapshot_id(90, PERIOD, 1)

    successes = []
    failures  = []
    barrier   = threading.Barrier(2)

    def try_write():
        barrier.wait()
        try:
            uri, _ = write_community_snapshot(
                iceberg_table, list(records), sid, EXPORTED, redis_client=redis_client
            )
            successes.append(uri)
        except (SnapshotLockError, SnapshotAlreadyExistsError) as e:
            failures.append(e)

    t1 = threading.Thread(target=try_write)
    t2 = threading.Thread(target=try_write)
    t1.start(); t2.start()
    t1.join(); t2.join()

    assert len(successes) == 1, f"Expected exactly one write to succeed, got {successes}"
    assert len(failures)  == 1, f"Expected exactly one failure, got {failures}"
