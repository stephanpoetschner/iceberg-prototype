# prototype/tests/test_billing_reader.py
import pytest
from datetime import datetime, timezone
from pydantic import ValidationError

from conftest import S3_CONFIG, S3_BUCKET, _s3_client

from billing_reader import BillingReader
from snapshot_id import make_snapshot_id
from synthetic import generate_records
from writer import write_snapshot, write_community_snapshot
from sidecar import BillingSnapshotMetadata, ECRegistrationRecord, MeteringPointRecord
from reader import query_community_billing, query_snap_nonsnap, query_registration_detail

PERIOD   = datetime(2026, 1, 1, tzinfo=timezone.utc)
EXPORTED = datetime(2026, 3, 26, 12, 0, 0, tzinfo=timezone.utc)

_ONE_REG = [ECRegistrationRecord(
    id=1, external_id="EXT-1", meteringpoint_id=10000,
    community_id=10,
    registered_from=datetime(2025, 1, 1, tzinfo=timezone.utc),
    registered_until=None,
)]
_ONE_MP = [MeteringPointRecord(id=10000, external_id="EXT-MP-10000", name="MP 10000", energy_direction=1)]


def _make_sidecar(ec_regs, mps, metadata_uri="file:///fake/v1.metadata.json"):
    return BillingSnapshotMetadata(
        snapshot_id=999,
        snapshot_version=1,
        community_id=10,
        community_name="Test Community",
        community_ec_id="AT001EC0001234",
        period_start=PERIOD,
        period_end=datetime(2026, 2, 1, tzinfo=timezone.utc),
        record_count=1,
        exported_at=EXPORTED,
        iceberg_metadata_uri=metadata_uri,
        ec_registrations=ec_regs,
        metering_points=mps,
    )


def test_empty_ec_registrations_raises():
    with pytest.raises(ValidationError):
        _make_sidecar(ec_regs=[], mps=_ONE_MP)


def test_empty_metering_points_raises():
    with pytest.raises(ValidationError):
        _make_sidecar(ec_regs=_ONE_REG, mps=[])


def test_from_sidecar_uri_file_loads_sidecar(iceberg_table, sidecar_dir):
    records = generate_records(community_id=70, period_start=PERIOD, num_points=1, seed=70)
    sid = make_snapshot_id(70, PERIOD, 1)
    reg_id = records[0]["ec_registration_id"]
    mp_id  = records[0]["metering_point_id"]

    write_snapshot(
        iceberg_table, records,
        snapshot_id=sid,
        snapshot_version=1,
        exported_at=EXPORTED,
        community_id=70,
        community_name="Test Community 70",
        community_ec_id="AT-EC-000070",
        period_start=PERIOD,
        period_end=datetime(2026, 2, 1, tzinfo=timezone.utc),
        ec_registrations=[ECRegistrationRecord(
            id=reg_id, external_id=f"EXT-{reg_id}", meteringpoint_id=mp_id,
            community_id=70,
            registered_from=datetime(2025, 1, 1, tzinfo=timezone.utc),
            registered_until=None,
        )],
        metering_points=[MeteringPointRecord(
            id=mp_id, external_id=f"EXT-MP-{mp_id}", name=f"MP {mp_id}", energy_direction=1,
        )],
        sidecar_dir=sidecar_dir,
    )

    sidecar_uri = f"file://{sidecar_dir}/{sid}.sidecar.json"
    reader = BillingReader.from_sidecar_uri(sidecar_uri)

    assert reader.sidecar.snapshot_id == sid
    assert reader.sidecar.community_id == 70


def _write_and_get_reader(iceberg_table, sidecar_dir, community_id, seed):
    records = generate_records(community_id=community_id, period_start=PERIOD, num_points=1, seed=seed)
    sid    = make_snapshot_id(community_id, PERIOD, 1)
    reg_id = records[0]["ec_registration_id"]
    mp_id  = records[0]["metering_point_id"]

    write_snapshot(
        iceberg_table, records,
        snapshot_id=sid,
        snapshot_version=1,
        exported_at=EXPORTED,
        community_id=community_id,
        community_name=f"Community {community_id}",
        community_ec_id=f"AT-EC-{community_id:06d}",
        period_start=PERIOD,
        period_end=datetime(2026, 2, 1, tzinfo=timezone.utc),
        ec_registrations=[ECRegistrationRecord(
            id=reg_id, external_id=f"EXT-{reg_id}", meteringpoint_id=mp_id,
            community_id=community_id,
            registered_from=datetime(2025, 1, 1, tzinfo=timezone.utc),
            registered_until=None,
        )],
        metering_points=[MeteringPointRecord(
            id=mp_id, external_id=f"EXT-MP-{mp_id}", name=f"MP {mp_id}", energy_direction=1,
        )],
        sidecar_dir=sidecar_dir,
    )

    sidecar_uri = f"file://{sidecar_dir}/{sid}.sidecar.json"
    reader = BillingReader.from_sidecar_uri(sidecar_uri)
    return reader, sid, reg_id


def test_community_billing_parity(iceberg_table, sidecar_dir):
    reader, sid, _ = _write_and_get_reader(iceberg_table, sidecar_dir, community_id=71, seed=71)
    expected = query_community_billing(
        reader.sidecar.iceberg_metadata_uri, community_id=71, snapshot_id=sid
    )
    result = reader.community_billing()
    assert result.num_rows == expected.num_rows
    assert sorted(result.column("snapshot_id").to_pylist()) == sorted(expected.column("snapshot_id").to_pylist())


def test_snap_nonsnap_parity(iceberg_table, sidecar_dir):
    reader, sid, _ = _write_and_get_reader(iceberg_table, sidecar_dir, community_id=72, seed=72)
    expected = query_snap_nonsnap(
        reader.sidecar.iceberg_metadata_uri, community_id=72, snapshot_id=sid
    )
    result = reader.snap_nonsnap()
    assert result.num_rows == expected.num_rows
    assert set(result.column_names) == set(expected.column_names)


def test_registration_detail_parity(iceberg_table, sidecar_dir):
    reader, sid, reg_id = _write_and_get_reader(iceberg_table, sidecar_dir, community_id=73, seed=73)
    expected = query_registration_detail(
        reader.sidecar.iceberg_metadata_uri, community_id=73, snapshot_id=sid, registration_id=reg_id
    )
    result = reader.registration_detail(reg_id)
    assert result.num_rows == expected.num_rows
    assert result.column("time").to_pylist() == expected.column("time").to_pylist()


@pytest.mark.s3
def test_from_sidecar_uri_s3_loads_and_queries(s3_iceberg_table):
    records = generate_records(community_id=80, period_start=PERIOD, num_points=1, seed=80)
    sid    = make_snapshot_id(80, PERIOD, 1)
    reg_id = records[0]["ec_registration_id"]
    mp_id  = records[0]["metering_point_id"]

    # Write Iceberg data to S3.
    uri, record_count = write_community_snapshot(
        s3_iceberg_table, records, snapshot_id=sid, exported_at=EXPORTED
    )
    assert uri.startswith("s3://"), f"Expected s3:// URI, got: {uri!r}"

    # Build and upload the sidecar JSON to S3.
    sidecar = BillingSnapshotMetadata(
        snapshot_id=sid,
        snapshot_version=1,
        community_id=80,
        community_name="S3 Test Community",
        community_ec_id="AT-EC-000080",
        period_start=PERIOD,
        period_end=datetime(2026, 2, 1, tzinfo=timezone.utc),
        record_count=record_count,
        exported_at=EXPORTED,
        iceberg_metadata_uri=uri,
        ec_registrations=[ECRegistrationRecord(
            id=reg_id, external_id=f"EXT-{reg_id}", meteringpoint_id=mp_id,
            community_id=80,
            registered_from=datetime(2025, 1, 1, tzinfo=timezone.utc),
            registered_until=None,
        )],
        metering_points=[MeteringPointRecord(
            id=mp_id, external_id=f"EXT-MP-{mp_id}", name=f"MP {mp_id}", energy_direction=1,
        )],
    )

    sidecar_key = f"sidecars/{sid}.sidecar.json"
    _s3_client().put_object(
        Bucket=S3_BUCKET,
        Key=sidecar_key,
        Body=sidecar.model_dump_json(indent=2).encode(),
    )

    # Load via BillingReader.from_sidecar_uri with s3:// URI.
    sidecar_uri = f"s3://{S3_BUCKET}/{sidecar_key}"
    reader = BillingReader.from_sidecar_uri(sidecar_uri, s3_config=S3_CONFIG)

    assert reader.sidecar.snapshot_id == sid
    result = reader.community_billing()
    assert result.num_rows == record_count


def test_billing_reader_pinned_to_v1_after_v2_commit(iceberg_table, sidecar_dir):
    """BillingReader instantiated from the v1 sidecar sees only v1 rows, even after v2 is committed."""
    records_v1 = generate_records(community_id=74, period_start=PERIOD, num_points=1, seed=74)
    records_v2 = generate_records(community_id=74, period_start=PERIOD, num_points=2, seed=75)

    sid_v1 = make_snapshot_id(74, PERIOD, 1)
    sid_v2 = make_snapshot_id(74, PERIOD, 2)
    reg_id = records_v1[0]["ec_registration_id"]
    mp_id  = records_v1[0]["metering_point_id"]

    # Write v1 — sidecar captures iceberg_metadata_uri at this point.
    write_snapshot(
        iceberg_table, records_v1,
        snapshot_id=sid_v1,
        snapshot_version=1,
        exported_at=EXPORTED,
        community_id=74,
        community_name="Pinning Test Community",
        community_ec_id="AT-EC-000074",
        period_start=PERIOD,
        period_end=datetime(2026, 2, 1, tzinfo=timezone.utc),
        ec_registrations=[ECRegistrationRecord(
            id=reg_id, external_id=f"EXT-{reg_id}", meteringpoint_id=mp_id,
            community_id=74,
            registered_from=datetime(2025, 1, 1, tzinfo=timezone.utc),
            registered_until=None,
        )],
        metering_points=[MeteringPointRecord(
            id=mp_id, external_id=f"EXT-MP-{mp_id}", name=f"MP {mp_id}", energy_direction=1,
        )],
        sidecar_dir=sidecar_dir,
    )

    # Commit v2 to the same table (different snapshot_id — no SnapshotAlreadyExistsError).
    write_community_snapshot(
        iceberg_table, records_v2, snapshot_id=sid_v2, exported_at=EXPORTED, snapshot_version=2
    )

    # Instantiate BillingReader from v1 sidecar URI — AFTER v2 commit.
    sidecar_uri = f"file://{sidecar_dir}/{sid_v1}.sidecar.json"
    reader = BillingReader.from_sidecar_uri(sidecar_uri)

    # Structural proof: v1 sidecar URI must differ from the current table head.
    # If BillingReader ever resolves to latest, this precondition ensures the test has
    # discriminating power — the URIs point to different metadata files.
    assert reader.sidecar.iceberg_metadata_uri != iceberg_table.metadata_location, (
        "Precondition failed: v2 commit must advance the table metadata URI so the "
        "pinning test can discriminate between v1 URI and latest URI."
    )

    result = reader.community_billing()
    returned_sids = set(result.column("snapshot_id").to_pylist())
    assert returned_sids == {sid_v1}, (
        f"Expected only v1 rows (snapshot_id={sid_v1}), got {returned_sids}. "
        "BillingReader must NOT auto-resolve to latest metadata."
    )
