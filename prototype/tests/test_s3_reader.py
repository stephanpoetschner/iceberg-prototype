# prototype/tests/test_s3_reader.py
"""Write to S3 (MinIO) via PyIceberg catalog; read back with DuckDB iceberg_scan.

Run only when MinIO is available: pytest -m s3
"""
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from snapshot_id import make_snapshot_id
from synthetic import generate_records
from writer import write_community_snapshot
from reader import query_community_billing, query_snap_nonsnap

PERIOD     = datetime(2026, 1, 1, tzinfo=timezone.utc)
EXPORTED   = datetime(2026, 3, 26, 12, 0, 0, tzinfo=timezone.utc)
S3_CONFIG  = {
    "endpoint":   "http://localhost:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "use_ssl":    False,
}


@pytest.mark.s3
def test_write_to_s3_and_read_with_duckdb(s3_iceberg_table):
    """Full round-trip: write community snapshot to S3, read back row count via DuckDB."""
    records     = generate_records(community_id=100, period_start=PERIOD, num_points=1, seed=100)
    snapshot_id = make_snapshot_id(100, PERIOD, 1)

    uri, record_count = write_community_snapshot(
        s3_iceberg_table, records, snapshot_id=snapshot_id, exported_at=EXPORTED
    )

    assert uri.startswith("s3://"), f"Expected s3:// URI, got: {uri!r}"

    result = query_community_billing(uri, community_id=100, snapshot_id=snapshot_id, s3_config=S3_CONFIG)
    assert len(result) == record_count


@pytest.mark.s3
def test_s3_snap_nonsnap_query(s3_iceberg_table):
    """SNAP/NON-SNAP aggregation works against S3-backed Iceberg table."""
    records     = generate_records(community_id=101, period_start=PERIOD, num_points=1, seed=101)
    snapshot_id = make_snapshot_id(101, PERIOD, 1)

    uri, _ = write_community_snapshot(
        s3_iceberg_table, records, snapshot_id=snapshot_id, exported_at=EXPORTED
    )

    result = query_snap_nonsnap(uri, community_id=101, snapshot_id=snapshot_id, s3_config=S3_CONFIG)
    assert len(result) > 0
    for row in result.to_pylist():
        assert row["total"] is not None or row["sum_SNAP"] is None
