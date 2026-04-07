# prototype/tests/test_atomic_visibility.py
"""
Atomic write visibility tests.

Iceberg's commit model: Parquet files are written first, then a new metadata.json
is written, then the catalog (PostgreSQL) is updated atomically.

A DuckDB reader using iceberg_scan(metadata_uri) reads a SPECIFIC metadata file.
Before the catalog is updated, the old URI returns 0 rows for the new snapshot_id.
After the catalog is updated, the new URI returns all rows immediately.
There is no intermediate state visible to a reader.
"""
import multiprocessing
import time
from datetime import datetime, timezone
from decimal import Decimal

import duckdb
import pytest

from snapshot_id import make_snapshot_id
from synthetic import generate_records
from writer import write_community_snapshot

PERIOD   = datetime(2026, 1, 1, tzinfo=timezone.utc)
EXPORTED = datetime(2026, 3, 26, 12, 0, 0, tzinfo=timezone.utc)


def test_one_export_creates_one_iceberg_snapshot(iceberg_table):
    """
    writer.py calls table.append() exactly once per export.
    Exactly one Iceberg snapshot must be created.
    If writer.py ever calls append() in a loop, this test catches it.
    """
    records = generate_records(community_id=800, period_start=PERIOD, num_points=1, seed=800)
    sid     = make_snapshot_id(800, PERIOD, 1)
    write_community_snapshot(iceberg_table, records, snapshot_id=sid, exported_at=EXPORTED)

    history = iceberg_table.history()
    assert len(history) == 1, (
        f"Expected exactly 1 Iceberg snapshot after 1 export, got {len(history)}. "
        "writer.py may be calling table.append() multiple times."
    )


def test_two_exports_create_two_iceberg_snapshots(iceberg_table):
    """Each export is one commit. Two exports = two Iceberg snapshots."""
    for v in range(1, 3):
        records = generate_records(community_id=801, period_start=PERIOD, num_points=1, seed=v)
        sid     = make_snapshot_id(801, PERIOD, v)
        write_community_snapshot(iceberg_table, records, snapshot_id=sid, exported_at=EXPORTED)

    history = iceberg_table.history()
    assert len(history) == 2, f"Expected 2 snapshots after 2 exports, got {len(history)}"


def test_pre_commit_metadata_uri_is_replaced_after_write(iceberg_table):
    """
    Before a write, capture metadata_location.
    After the write, metadata_location must be a different file.
    This proves the commit created a new metadata file (Iceberg's atomicity mechanism).
    """
    pre_write_uri = iceberg_table.metadata_location

    records = generate_records(community_id=802, period_start=PERIOD, num_points=1, seed=802)
    sid     = make_snapshot_id(802, PERIOD, 1)
    post_write_uri, _ = write_community_snapshot(
        iceberg_table, records, snapshot_id=sid, exported_at=EXPORTED
    )

    assert pre_write_uri != post_write_uri, (
        "metadata_location did not change after write. "
        "The commit may not have completed correctly."
    )


def test_old_metadata_uri_returns_zero_rows_for_new_snapshot(iceberg_table):
    """
    A DuckDB reader holding the old metadata_location will not see any rows
    from a write that committed after it captured the URI.
    This is Iceberg's snapshot isolation: old URIs are immutable.
    """
    pre_write_uri = iceberg_table.metadata_location

    records = generate_records(community_id=803, period_start=PERIOD, num_points=1, seed=803)
    sid     = make_snapshot_id(803, PERIOD, 1)
    write_community_snapshot(iceberg_table, records, snapshot_id=sid, exported_at=EXPORTED)

    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")

    # The old URI must return 0 rows for the new snapshot_id
    count_old_uri = conn.execute(
        f"SELECT COUNT(*) FROM iceberg_scan('{pre_write_uri}') WHERE snapshot_id = {sid}"
    ).fetchone()[0]
    assert count_old_uri == 0, (
        f"Old metadata URI returned {count_old_uri} rows for a snapshot committed after it was captured. "
        "Iceberg snapshot isolation is broken."
    )

    # The new URI must return all rows
    new_uri = iceberg_table.metadata_location
    count_new_uri = conn.execute(
        f"SELECT COUNT(*) FROM iceberg_scan('{new_uri}') WHERE snapshot_id = {sid}"
    ).fetchone()[0]
    assert count_new_uri == len(records)


def test_post_commit_count_is_exact_not_partial(iceberg_table):
    """
    Immediately after write_community_snapshot() returns, COUNT(*) must equal
    the full expected count — never a partial value.
    This tests that the single table.append() commit is truly atomic from the
    reader's perspective.
    """
    records     = generate_records(community_id=804, period_start=PERIOD, num_points=1, seed=804)
    sid         = make_snapshot_id(804, PERIOD, 1)
    uri, _      = write_community_snapshot(iceberg_table, records, snapshot_id=sid, exported_at=EXPORTED)

    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    count = conn.execute(
        f"SELECT COUNT(*) FROM iceberg_scan('{uri}') WHERE snapshot_id = {sid}"
    ).fetchone()[0]

    assert count == len(records), (
        f"Post-commit count is {count}, expected {len(records)}. "
        "The write committed partially — possible multiple table.append() calls."
    )


def _subprocess_writer(table_ident: str, warehouse_dir: str, community_id: int, result_queue):
    """Write 200-point outlier in a subprocess; puts (uri, sid, count) in result_queue."""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

    from datetime import datetime, timezone
    from pyiceberg.catalog import load_catalog
    from snapshot_id import make_snapshot_id
    from synthetic import generate_records
    from writer import write_community_snapshot

    CATALOG_URI = "postgresql+psycopg2://iceberg:iceberg@localhost:5432/iceberg_test"
    period      = datetime(2026, 1, 1, tzinfo=timezone.utc)
    exported_at = datetime(2026, 3, 26, 12, 0, 0, tzinfo=timezone.utc)

    cat     = load_catalog("test", **{"type": "sql", "uri": CATALOG_URI, "warehouse": f"file://{warehouse_dir}"})
    table   = cat.load_table(table_ident)
    records = generate_records(community_id=community_id, period_start=period, num_points=200, seed=community_id)
    sid     = make_snapshot_id(community_id, period, 1)
    uri, _  = write_community_snapshot(table, records, snapshot_id=sid, exported_at=exported_at)
    result_queue.put({"uri": uri, "sid": sid, "count": len(records)})


@pytest.mark.slow
def test_subprocess_write_count_is_never_partial(iceberg_table, warehouse_dir):
    """
    Write a 200-point outlier (~3M records) in a subprocess.
    While it runs, the main process polls COUNT(*) via DuckDB using the pre-write
    metadata URI. All observed counts must be 0 (not yet committed) or the full count
    (after commit) — never a partial count.
    """
    table_ident   = ".".join(iceberg_table._identifier)
    pre_write_uri = iceberg_table.metadata_location
    expected_count = 200 * 96 * 31 * 5  # 2,976,000

    ctx    = multiprocessing.get_context("spawn")
    q      = ctx.Queue()
    writer = ctx.Process(
        target=_subprocess_writer,
        args=(table_ident, warehouse_dir, 850, q),
    )
    writer.start()

    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")

    observed_partial = []
    deadline = time.time() + 300  # poll for up to 5 minutes

    while writer.is_alive() and time.time() < deadline:
        # Using pre_write_uri: should always see 0 rows (immutable snapshot)
        try:
            count = conn.execute(
                f"SELECT COUNT(*) FROM iceberg_scan('{pre_write_uri}') "
                f"WHERE snapshot_id = {make_snapshot_id(850, PERIOD, 1)}"
            ).fetchone()[0]
            if count not in (0, expected_count):
                observed_partial.append(count)
        except Exception:
            pass  # file may not exist yet
        time.sleep(0.2)

    writer.join(timeout=360)
    assert writer.exitcode == 0, "Writer subprocess failed"

    result = q.get_nowait()
    assert result["count"] == expected_count

    # Verify the final count is correct
    final_count = conn.execute(
        f"SELECT COUNT(*) FROM iceberg_scan('{result['uri']}') "
        f"WHERE snapshot_id = {result['sid']}"
    ).fetchone()[0]
    assert final_count == expected_count

    assert not observed_partial, (
        f"Partial counts observed during write: {observed_partial}. "
        "The write is NOT atomic — reader saw intermediate state."
    )
