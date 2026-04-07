# prototype/tests/test_reader.py
from datetime import datetime, timezone
import pytest

from snapshot_id import make_snapshot_id
from synthetic import generate_records
from writer import write_community_snapshot
from reader import (
    query_community_billing,
    query_snap_nonsnap,
    query_registration_detail,
    query_cross_community,
)

PERIOD   = datetime(2026, 1, 1, tzinfo=timezone.utc)
EXPORTED = datetime(2026, 3, 26, 12, 0, 0, tzinfo=timezone.utc)


def test_query_community_billing_returns_correct_rows(iceberg_table):
    records = generate_records(community_id=20, period_start=PERIOD, num_points=1, seed=20)
    snapshot_id = make_snapshot_id(20, PERIOD, 1)
    uri, _ = write_community_snapshot(iceberg_table, records, snapshot_id=snapshot_id, exported_at=EXPORTED)

    result = query_community_billing(uri, community_id=20, snapshot_id=snapshot_id)
    assert len(result) == len(records)


def test_query_snap_nonsnap_columns_present(iceberg_table):
    records = generate_records(community_id=21, period_start=PERIOD, num_points=2, seed=21)
    snapshot_id = make_snapshot_id(21, PERIOD, 1)
    uri, _ = write_community_snapshot(iceberg_table, records, snapshot_id=snapshot_id, exported_at=EXPORTED)

    result = query_snap_nonsnap(uri, community_id=21, snapshot_id=snapshot_id)
    assert "total" in result.column_names
    assert "sum_SNAP" in result.column_names
    assert "sum_NONSNAP" in result.column_names
    assert "ec_registration_id" in result.column_names
    assert "record_type" in result.column_names


def test_query_snap_nonsnap_totals_sum_correctly(iceberg_table):
    records = generate_records(community_id=22, period_start=PERIOD, num_points=2, seed=22)
    snapshot_id = make_snapshot_id(22, PERIOD, 1)
    uri, _ = write_community_snapshot(iceberg_table, records, snapshot_id=snapshot_id, exported_at=EXPORTED)

    result = query_snap_nonsnap(uri, community_id=22, snapshot_id=snapshot_id)
    for i in range(len(result)):
        row = result.slice(i, 1)
        total   = row.column("total")[0].as_py()
        snap    = row.column("sum_SNAP")[0].as_py()
        nonsnap = row.column("sum_NONSNAP")[0].as_py()
        if total is not None:
            from decimal import Decimal
            snap_val    = snap    if snap    is not None else Decimal(0)
            nonsnap_val = nonsnap if nonsnap is not None else Decimal(0)
            assert abs(total - (snap_val + nonsnap_val)) < Decimal("0.000001")


def test_query_registration_detail_returns_ordered_rows(iceberg_table):
    records = generate_records(community_id=23, period_start=PERIOD, num_points=1, seed=23)
    snapshot_id = make_snapshot_id(23, PERIOD, 1)
    uri, _ = write_community_snapshot(iceberg_table, records, snapshot_id=snapshot_id, exported_at=EXPORTED)

    registration_id = records[0]["ec_registration_id"]
    result = query_registration_detail(uri, community_id=23, snapshot_id=snapshot_id, registration_id=registration_id)

    assert "time" in result.column_names
    assert "record_type" in result.column_names
    assert "value" in result.column_names
    assert "value_type" in result.column_names

    times = result.column("time").to_pylist()
    assert times == sorted(times)


def test_query_cross_community_returns_all_communities(iceberg_table):
    snapshot_ids = {}
    uris = []
    for cid in [30, 31, 32]:
        records = generate_records(community_id=cid, period_start=PERIOD, num_points=1, seed=cid)
        sid = make_snapshot_id(cid, PERIOD, 1)
        uri, _ = write_community_snapshot(iceberg_table, records, snapshot_id=sid, exported_at=EXPORTED)
        snapshot_ids[cid] = sid
        uris.append(uri)

    result = query_cross_community(uri, snapshot_ids=list(snapshot_ids.values()))
    community_ids_returned = set(result.column("community_id").to_pylist())
    assert {30, 31, 32}.issubset(community_ids_returned)


def test_missing_snapshot_id_filter_returns_all_versions(iceberg_table):
    """Documents the failure mode: querying without snapshot_id returns mixed versions."""
    import duckdb
    import pyarrow as pa
    from schema import ARROW_SCHEMA
    from writer import _build_arrow_table

    records_v1 = generate_records(community_id=40, period_start=PERIOD, num_points=1, seed=40)
    records_v2 = generate_records(community_id=40, period_start=PERIOD, num_points=1, seed=41)

    sid_v1 = make_snapshot_id(40, PERIOD, 1)
    sid_v2 = make_snapshot_id(40, PERIOD, 2)

    uri, _ = write_community_snapshot(iceberg_table, records_v1, snapshot_id=sid_v1, exported_at=EXPORTED)

    arrow_v2 = _build_arrow_table(records_v2, snapshot_id=sid_v2, exported_at=EXPORTED)
    cols = {name: arrow_v2.column(name) for name in arrow_v2.schema.names}
    cols["snapshot_version"] = pa.array([2] * len(records_v2), type=pa.int32())
    arrow_v2 = pa.table(cols, schema=ARROW_SCHEMA)
    iceberg_table.append(arrow_v2)

    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")

    count_v1 = conn.execute(
        f"SELECT COUNT(*) FROM iceberg_scan('{uri}') WHERE community_id = 40 AND snapshot_id = {sid_v1}"
    ).fetchone()[0]
    assert count_v1 == len(records_v1)

    # Use the latest metadata URI (after v2 append) to observe the mixed-version failure mode.
    # The uri captured from v1 only sees v1 data; the latest metadata sees both versions.
    latest_uri = iceberg_table.metadata_location
    count_all = conn.execute(
        f"SELECT COUNT(*) FROM iceberg_scan('{latest_uri}') WHERE community_id = 40"
    ).fetchone()[0]
    assert count_all == len(records_v1) + len(records_v2)


def test_two_versions_are_independent(iceberg_table):
    records_v1 = generate_records(community_id=41, period_start=PERIOD, num_points=1, seed=41)
    records_v2 = generate_records(community_id=41, period_start=PERIOD, num_points=1, seed=42)

    sid_v1 = make_snapshot_id(41, PERIOD, 1)
    sid_v2 = make_snapshot_id(41, PERIOD, 2)

    uri, _ = write_community_snapshot(iceberg_table, records_v1, snapshot_id=sid_v1, exported_at=EXPORTED)
    write_community_snapshot(iceberg_table, records_v2, snapshot_id=sid_v2, exported_at=EXPORTED)

    result_v1 = query_community_billing(uri, community_id=41, snapshot_id=sid_v1)
    result_v2 = query_community_billing(uri, community_id=41, snapshot_id=sid_v2)

    assert len(result_v1) == len(records_v1)
    assert len(result_v2) == len(records_v2)
    v1_sids = set(result_v1.column("snapshot_id").to_pylist())
    v2_sids = set(result_v2.column("snapshot_id").to_pylist())
    assert v1_sids == {sid_v1}
    assert v2_sids == {sid_v2}
