import os
import pyarrow as pa
from datetime import datetime
from decimal import Decimal

from schema import ARROW_SCHEMA
from snapshot_id import SnapshotAlreadyExistsError
from pyiceberg.expressions import EqualTo
from sidecar import BillingSnapshotMetadata, ECRegistrationRecord, MeteringPointRecord, write_sidecar, read_sidecar
from lock import snapshot_lock

CHUNK_SIZE = 100_000


def _build_arrow_table(records, snapshot_id: int, exported_at: datetime, snapshot_version: int = 1) -> pa.Table:
    """Convert a records iterable into a PyArrow Table matching ARROW_SCHEMA.

    Accepts any iterable (list, generator, iterator). Processes CHUNK_SIZE records
    at a time and replaces each chunk with an empty list after building its Arrow
    batch, allowing the Python dict objects to be garbage-collected before the next
    chunk begins. This keeps peak memory bounded to ~CHUNK_SIZE dicts + all Arrow
    batches when the input is a generator (e.g. generate_records_iter).
    """
    batches = []
    chunk = []
    for r in records:
        chunk.append(r)
        if len(chunk) >= CHUNK_SIZE:
            n = len(chunk)
            batches.append(pa.record_batch(
                {
                    "snapshot_id":        pa.array([snapshot_id] * n,                             type=pa.int64()),
                    "community_id":       pa.array([r["community_id"] for r in chunk],            type=pa.int32()),
                    "time":               pa.array([r["time"] for r in chunk],                    type=pa.timestamp("us", tz="UTC")),
                    "record_type":        pa.array([r["record_type"] for r in chunk],             type=pa.int32()),
                    "ec_registration_id": pa.array([r["ec_registration_id"] for r in chunk],      type=pa.int64()),
                    "metering_point_id":  pa.array([r["metering_point_id"] for r in chunk],       type=pa.int64()),
                    "value":              pa.array([r["value"] for r in chunk],                   type=pa.decimal128(22, 6)),
                    "value_type":         pa.array([r["value_type"] for r in chunk],              type=pa.int32()),
                    "message_created_at": pa.array([r["message_created_at"] for r in chunk],      type=pa.timestamp("us", tz="UTC")),
                    "exported_at":        pa.array([exported_at] * n,                             type=pa.timestamp("us", tz="UTC")),
                    "snapshot_version":   pa.array([snapshot_version] * n,                         type=pa.int32()),
                },
                schema=ARROW_SCHEMA,
            ))
            chunk = []  # release references so Python dicts can be GC'd
    if chunk:
        n = len(chunk)
        batches.append(pa.record_batch(
            {
                "snapshot_id":        pa.array([snapshot_id] * n,                             type=pa.int64()),
                "community_id":       pa.array([r["community_id"] for r in chunk],            type=pa.int32()),
                "time":               pa.array([r["time"] for r in chunk],                    type=pa.timestamp("us", tz="UTC")),
                "record_type":        pa.array([r["record_type"] for r in chunk],             type=pa.int32()),
                "ec_registration_id": pa.array([r["ec_registration_id"] for r in chunk],      type=pa.int64()),
                "metering_point_id":  pa.array([r["metering_point_id"] for r in chunk],       type=pa.int64()),
                "value":              pa.array([r["value"] for r in chunk],                   type=pa.decimal128(22, 6)),
                "value_type":         pa.array([r["value_type"] for r in chunk],              type=pa.int32()),
                "message_created_at": pa.array([r["message_created_at"] for r in chunk],      type=pa.timestamp("us", tz="UTC")),
                "exported_at":        pa.array([exported_at] * n,                             type=pa.timestamp("us", tz="UTC")),
                "snapshot_version":   pa.array([snapshot_version] * n,                         type=pa.int32()),
            },
            schema=ARROW_SCHEMA,
        ))
    return pa.Table.from_batches(batches, schema=ARROW_SCHEMA)


def write_community_snapshot(
    table,
    records,
    snapshot_id: int,
    exported_at: datetime,
    snapshot_version: int = 1,
    redis_client=None,
) -> tuple[str, int]:
    """Write records for one community-period as a single atomic Iceberg commit.

    records may be a list[dict] or any iterator/generator of dicts.

    If redis_client is provided, acquires a distributed lock on snapshot_id before
    the idempotency check, preventing concurrent callers from both writing the same
    snapshot. Without redis_client, the check is optimistic (read-then-write).

    Returns (iceberg_metadata_uri, record_count).
    Raises SnapshotAlreadyExistsError if the snapshot is already committed.
    Raises SnapshotLockError if redis_client is provided and the lock is held.
    """
    if redis_client is not None:
        with snapshot_lock(redis_client, snapshot_id):
            return _do_write(table, records, snapshot_id, exported_at, snapshot_version)
    return _do_write(table, records, snapshot_id, exported_at, snapshot_version)


def _do_write(table, records, snapshot_id: int, exported_at: datetime, snapshot_version: int) -> tuple[str, int]:
    existing = table.scan(
        row_filter=EqualTo("snapshot_id", snapshot_id),
    ).to_arrow()
    if len(existing) > 0:
        raise SnapshotAlreadyExistsError(snapshot_id)

    arrow_table = _build_arrow_table(records, snapshot_id=snapshot_id, exported_at=exported_at, snapshot_version=snapshot_version)
    table.append(arrow_table)
    return table.metadata_location, arrow_table.num_rows


def write_snapshot(
    table,
    records: list[dict],
    *,
    snapshot_id: int,
    snapshot_version: int = 1,
    exported_at: datetime,
    community_id: int,
    community_name: str,
    community_ec_id: str,
    period_start: datetime,
    period_end: datetime,
    ec_registrations: list[ECRegistrationRecord],
    metering_points: list[MeteringPointRecord],
    sidecar_dir: str,
    redis_client=None,
) -> BillingSnapshotMetadata:
    """Write records as an Iceberg commit and persist a BillingSnapshotMetadata sidecar.

    Fully idempotent: a retry with the same snapshot_id always succeeds and returns
    a valid BillingSnapshotMetadata, regardless of which step a prior crash interrupted.

    Three states:
    - First write: write_community_snapshot() succeeds → build metadata, write sidecar, return.
    - Clean retry: SnapshotAlreadyExistsError + sidecar exists → read and return existing sidecar.
    - Crash recovery: SnapshotAlreadyExistsError + sidecar missing → scan table for record_count,
      use table.metadata_location, write sidecar, return.

    Sidecar is written to <sidecar_dir>/<snapshot_id>.sidecar.json.
    """
    sidecar_path = os.path.join(sidecar_dir, f"{snapshot_id}.sidecar.json")

    try:
        iceberg_metadata_uri, record_count = write_community_snapshot(
            table, records, snapshot_id, exported_at, snapshot_version, redis_client=redis_client
        )
    except SnapshotAlreadyExistsError:
        if os.path.exists(sidecar_path):
            return read_sidecar(sidecar_path)           # clean retry
        # crash recovery: committed, sidecar missing
        existing = table.scan(row_filter=EqualTo("snapshot_id", snapshot_id)).to_arrow()
        record_count = len(existing)
        iceberg_metadata_uri = table.metadata_location

    metadata = BillingSnapshotMetadata(
        snapshot_id=snapshot_id,
        snapshot_version=snapshot_version,
        community_id=community_id,
        community_name=community_name,
        community_ec_id=community_ec_id,
        period_start=period_start,
        period_end=period_end,
        record_count=record_count,
        exported_at=exported_at,
        iceberg_metadata_uri=iceberg_metadata_uri,
        ec_registrations=ec_registrations,
        metering_points=metering_points,
    )
    write_sidecar(sidecar_path, metadata)
    return metadata
