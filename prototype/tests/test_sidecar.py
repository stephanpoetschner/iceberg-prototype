# prototype/tests/test_sidecar.py
import json
import os
import tempfile
from datetime import datetime, timezone

import duckdb
import pytest

from snapshot_id import make_snapshot_id
from synthetic import generate_records
from writer import write_community_snapshot
from sidecar import (
    BillingSnapshotMetadata,
    ECRegistrationRecord,
    MeteringPointRecord,
    write_sidecar,
    read_sidecar,
)

PERIOD   = datetime(2026, 1, 1, tzinfo=timezone.utc)
EXPORTED = datetime(2026, 3, 26, 12, 0, 0, tzinfo=timezone.utc)


def _make_metadata(snapshot_id: int, record_count: int, metadata_uri: str) -> BillingSnapshotMetadata:
    return BillingSnapshotMetadata(
        snapshot_id=snapshot_id,
        snapshot_version=1,
        community_id=10,
        community_name="Test Community",
        community_ec_id="AT001EC0001234",
        period_start=PERIOD,
        period_end=datetime(2026, 2, 1, tzinfo=timezone.utc),
        record_count=record_count,
        exported_at=EXPORTED,
        iceberg_metadata_uri=metadata_uri,
        ec_registrations=[
            ECRegistrationRecord(
                id=1, egon_id=100, meteringpoint_id=10000,
                community_id=10,
                registered_from=datetime(2025, 1, 1, tzinfo=timezone.utc),
                registered_until=None,
            )
        ],
        metering_points=[
            MeteringPointRecord(id=10000, egon_id=200, name="AT0010000000000000000001234567890", energy_direction=1)
        ],
    )


def test_sidecar_round_trips_through_pydantic(iceberg_table):
    records = generate_records(community_id=10, period_start=PERIOD, num_points=1, seed=10)
    snapshot_id = make_snapshot_id(10, PERIOD, 1)
    uri, _ = write_community_snapshot(iceberg_table, records, snapshot_id=snapshot_id, exported_at=EXPORTED)

    meta = _make_metadata(snapshot_id, len(records), uri)

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "snapshot.metadata.json")
        write_sidecar(path, meta)

        loaded = read_sidecar(path)
        assert loaded.snapshot_id == snapshot_id
        assert loaded.record_count == len(records)
        assert loaded.iceberg_metadata_uri == uri
        assert loaded.community_name == "Test Community"
        assert len(loaded.ec_registrations) == 1
        assert len(loaded.metering_points) == 1


def test_sidecar_is_valid_json(iceberg_table):
    records = generate_records(community_id=11, period_start=PERIOD, num_points=1, seed=11)
    snapshot_id = make_snapshot_id(11, PERIOD, 1)
    uri, _ = write_community_snapshot(iceberg_table, records, snapshot_id=snapshot_id, exported_at=EXPORTED)
    meta = _make_metadata(snapshot_id, len(records), uri)

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "snapshot.metadata.json")
        write_sidecar(path, meta)
        with open(path) as f:
            data = json.load(f)
        assert data["snapshot_id"] == snapshot_id


def test_sidecar_record_count_matches_actual_data(iceberg_table):
    records = generate_records(community_id=12, period_start=PERIOD, num_points=2, seed=12)
    snapshot_id = make_snapshot_id(12, PERIOD, 1)
    uri, _ = write_community_snapshot(iceberg_table, records, snapshot_id=snapshot_id, exported_at=EXPORTED)
    meta = _make_metadata(snapshot_id, len(records), uri)

    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    actual_count = conn.execute(
        f"SELECT COUNT(*) FROM iceberg_scan('{uri}') WHERE snapshot_id = {snapshot_id}"
    ).fetchone()[0]

    assert meta.record_count == actual_count


def test_sidecar_metadata_uri_points_to_valid_file(iceberg_table):
    records = generate_records(community_id=13, period_start=PERIOD, num_points=1, seed=13)
    snapshot_id = make_snapshot_id(13, PERIOD, 1)
    uri, _ = write_community_snapshot(iceberg_table, records, snapshot_id=snapshot_id, exported_at=EXPORTED)
    meta = _make_metadata(snapshot_id, len(records), uri)

    # Strip file:// prefix for os.path.exists check
    local_path = meta.iceberg_metadata_uri.removeprefix("file://")
    assert os.path.exists(local_path), f"Metadata file not found: {local_path}"


# ── write_snapshot() idempotency tests ────────────────────────────────────────

from writer import write_snapshot


def _write_snapshot_kwargs(snapshot_id, sidecar_dir):
    return dict(
        snapshot_id=snapshot_id,
        snapshot_version=1,
        exported_at=EXPORTED,
        community_id=20,
        community_name="Idempotency Test Community",
        community_ec_id="AT001EC0099999",
        period_start=PERIOD,
        period_end=datetime(2026, 2, 1, tzinfo=timezone.utc),
        ec_registrations=[
            ECRegistrationRecord(
                id=1, egon_id=100, meteringpoint_id=10000,
                community_id=20,
                registered_from=datetime(2025, 1, 1, tzinfo=timezone.utc),
                registered_until=None,
            )
        ],
        metering_points=[
            MeteringPointRecord(id=10000, egon_id=200, name="AT0010000000000000000001234567890", energy_direction=1)
        ],
        sidecar_dir=sidecar_dir,
    )


def test_write_snapshot_clean_idempotent_retry(iceberg_table, sidecar_dir):
    records = generate_records(community_id=20, period_start=PERIOD, num_points=1, seed=20)
    snapshot_id = make_snapshot_id(20, PERIOD, 1)
    kwargs = _write_snapshot_kwargs(snapshot_id, sidecar_dir)

    meta1 = write_snapshot(iceberg_table, records, **kwargs)
    records2 = generate_records(community_id=20, period_start=PERIOD, num_points=1, seed=20)
    meta2 = write_snapshot(iceberg_table, records2, **kwargs)

    assert meta1 == meta2
    sidecar_path = os.path.join(sidecar_dir, f"{snapshot_id}.sidecar.json")
    assert os.path.exists(sidecar_path)


def test_write_snapshot_crash_recovery(iceberg_table, sidecar_dir):
    records = generate_records(community_id=21, period_start=PERIOD, num_points=2, seed=21)
    snapshot_id = make_snapshot_id(21, PERIOD, 1)

    # Simulate crash: Iceberg committed, sidecar never written
    write_community_snapshot(iceberg_table, records, snapshot_id=snapshot_id, exported_at=EXPORTED)
    sidecar_path = os.path.join(sidecar_dir, f"{snapshot_id}.sidecar.json")
    assert not os.path.exists(sidecar_path)

    # Recovery: write_snapshot with same snapshot_id should succeed
    records2 = generate_records(community_id=21, period_start=PERIOD, num_points=2, seed=21)
    kwargs = _write_snapshot_kwargs(snapshot_id, sidecar_dir)
    kwargs["community_id"] = 21
    meta = write_snapshot(iceberg_table, records2, **kwargs)

    assert os.path.exists(sidecar_path)
    assert meta.snapshot_id == snapshot_id
    assert meta.snapshot_version == 1
    assert meta.record_count == len(list(generate_records(community_id=21, period_start=PERIOD, num_points=2, seed=21)))
