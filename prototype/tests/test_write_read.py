# prototype/tests/test_write_read.py
import os
from datetime import datetime, timezone
from decimal import Decimal
import duckdb
import pytest

from snapshot_id import make_snapshot_id, SnapshotAlreadyExistsError
from synthetic import generate_records
from writer import write_community_snapshot, write_snapshot
from sidecar import ECRegistrationRecord, MeteringPointRecord, read_sidecar


PERIOD    = datetime(2026, 1, 1, tzinfo=timezone.utc)
EXPORTED  = datetime(2026, 3, 26, 12, 0, 0, tzinfo=timezone.utc)


def test_write_returns_metadata_uri(iceberg_table):
    records = generate_records(community_id=1, period_start=PERIOD, num_points=1, seed=0)
    snapshot_id = make_snapshot_id(1, PERIOD, 1)
    uri, _ = write_community_snapshot(iceberg_table, records, snapshot_id=snapshot_id, exported_at=EXPORTED)
    assert uri.startswith("file://")
    assert uri.endswith(".metadata.json")


def test_row_count_matches_after_write(iceberg_table):
    records = generate_records(community_id=2, period_start=PERIOD, num_points=1, seed=1)
    snapshot_id = make_snapshot_id(2, PERIOD, 1)
    uri, _ = write_community_snapshot(iceberg_table, records, snapshot_id=snapshot_id, exported_at=EXPORTED)

    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    result = conn.execute(
        f"SELECT COUNT(*) FROM iceberg_scan('{uri}') WHERE community_id = 2 AND snapshot_id = {snapshot_id}"
    ).fetchone()
    assert result[0] == len(records)


def test_idempotency_rejects_duplicate_write(iceberg_table):
    records = generate_records(community_id=3, period_start=PERIOD, num_points=1, seed=2)
    snapshot_id = make_snapshot_id(3, PERIOD, 1)
    write_community_snapshot(iceberg_table, records, snapshot_id=snapshot_id, exported_at=EXPORTED)

    with pytest.raises(SnapshotAlreadyExistsError):
        write_community_snapshot(iceberg_table, records, snapshot_id=snapshot_id, exported_at=EXPORTED)


def test_idempotency_does_not_change_row_count(iceberg_table):
    records = generate_records(community_id=4, period_start=PERIOD, num_points=1, seed=3)
    snapshot_id = make_snapshot_id(4, PERIOD, 1)
    uri, _ = write_community_snapshot(iceberg_table, records, snapshot_id=snapshot_id, exported_at=EXPORTED)

    try:
        write_community_snapshot(iceberg_table, records, snapshot_id=snapshot_id, exported_at=EXPORTED)
    except SnapshotAlreadyExistsError:
        pass

    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    count = conn.execute(
        f"SELECT COUNT(*) FROM iceberg_scan('{uri}') WHERE snapshot_id = {snapshot_id}"
    ).fetchone()[0]
    assert count == len(records)


def test_exported_at_is_uniform_across_all_rows(iceberg_table):
    records = generate_records(community_id=5, period_start=PERIOD, num_points=1, seed=4)
    snapshot_id = make_snapshot_id(5, PERIOD, 1)
    uri, _ = write_community_snapshot(iceberg_table, records, snapshot_id=snapshot_id, exported_at=EXPORTED)

    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    distinct_timestamps = conn.execute(
        f"SELECT COUNT(DISTINCT exported_at) FROM iceberg_scan('{uri}') WHERE snapshot_id = {snapshot_id}"
    ).fetchone()[0]
    assert distinct_timestamps == 1


def test_null_values_survive_round_trip(iceberg_table):
    records = generate_records(community_id=6, period_start=PERIOD, num_points=1, seed=5)
    snapshot_id = make_snapshot_id(6, PERIOD, 1)
    uri, _ = write_community_snapshot(iceberg_table, records, snapshot_id=snapshot_id, exported_at=EXPORTED)

    null_expected = sum(1 for r in records if r["value"] is None)
    assert null_expected > 0, "Test setup error: seed produced no nulls"

    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    null_actual = conn.execute(
        f"SELECT COUNT(*) FROM iceberg_scan('{uri}') WHERE snapshot_id = {snapshot_id} AND value IS NULL"
    ).fetchone()[0]
    assert null_actual == null_expected


def test_decimal_precision_survives_round_trip(iceberg_table):
    from datetime import timedelta
    import pyarrow as pa
    from schema import ARROW_SCHEMA

    # Write a single record with a known 6-decimal value
    exported_at = datetime(2026, 3, 26, 12, 0, 0, tzinfo=timezone.utc)
    slot_time   = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    snapshot_id = make_snapshot_id(7, PERIOD, 1)

    record = {
        "community_id":       7,
        "time":               slot_time,
        "record_type":        1,
        "ec_registration_id": 7000,
        "metering_point_id":  70000,
        "value":              Decimal("1.123456"),
        "value_type":         1,
        "message_created_at": exported_at,
    }

    uri, _ = write_community_snapshot(iceberg_table, [record], snapshot_id=snapshot_id, exported_at=EXPORTED)

    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    row = conn.execute(
        f"SELECT value FROM iceberg_scan('{uri}') WHERE snapshot_id = {snapshot_id}"
    ).fetchone()
    assert row[0] == Decimal("1.123456")


def test_write_snapshot_sidecar_integration(iceberg_table, sidecar_dir):
    community_id = 20
    snapshot_id  = make_snapshot_id(community_id, PERIOD, 1)
    records      = generate_records(community_id=community_id, period_start=PERIOD, num_points=2, seed=20)

    ec_registrations = [
        ECRegistrationRecord(
            id=1, external_id="EXT-1", meteringpoint_id=20000,
            community_id=community_id,
            registered_from=datetime(2025, 1, 1, tzinfo=timezone.utc),
            registered_until=None,
        )
    ]
    metering_points = [
        MeteringPointRecord(id=20000, external_id="EXT-MP-20000", name="AT0020000000000000000001234567890", energy_direction=1)
    ]

    metadata = write_snapshot(
        iceberg_table,
        records,
        snapshot_id=snapshot_id,
        snapshot_version=1,
        exported_at=EXPORTED,
        community_id=community_id,
        community_name="Test Community Alpha",
        community_ec_id="AT001EC0000020",
        period_start=PERIOD,
        period_end=datetime(2026, 2, 1, tzinfo=timezone.utc),
        ec_registrations=ec_registrations,
        metering_points=metering_points,
        sidecar_dir=sidecar_dir,
    )

    # 1. iceberg_metadata_uri points to a file that exists on disk
    local_path = metadata.iceberg_metadata_uri.removeprefix("file://")
    assert os.path.exists(local_path), f"Metadata file not found: {local_path}"

    # 2. record_count matches actual data in Iceberg
    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    actual_count = conn.execute(
        f"SELECT COUNT(*) FROM iceberg_scan('{metadata.iceberg_metadata_uri}') WHERE snapshot_id = {snapshot_id}"
    ).fetchone()[0]
    assert metadata.record_count == len(records)
    assert metadata.record_count == actual_count

    # 3. sidecar round-trips to the same BillingSnapshotMetadata
    sidecar_path = os.path.join(sidecar_dir, f"{snapshot_id}.sidecar.json")
    loaded = read_sidecar(sidecar_path)
    assert loaded.snapshot_id        == metadata.snapshot_id
    assert loaded.record_count       == metadata.record_count
    assert loaded.iceberg_metadata_uri == metadata.iceberg_metadata_uri
    assert loaded.community_name     == "Test Community Alpha"
    assert len(loaded.ec_registrations) == 1
    assert len(loaded.metering_points)  == 1
