import hashlib
from datetime import datetime


def make_snapshot_id(community_id: int, period_start: datetime, snapshot_version: int = 1) -> int:
    """Deterministic snapshot ID derived from community, period, and version.

    Masked to signed int64 range to match Iceberg LongType.
    Same inputs always produce the same ID — enables idempotency checks.
    """
    if period_start.tzinfo is None:
        raise ValueError("period_start must be timezone-aware")
    key = f"{community_id}:{period_start.isoformat()}:{snapshot_version}"
    return int(hashlib.sha256(key.encode()).hexdigest()[:16], 16) & 0x7FFFFFFFFFFFFFFF


class SnapshotAlreadyExistsError(Exception):
    def __init__(self, snapshot_id: int):
        super().__init__(f"Snapshot {snapshot_id} already exists in the table")
        self.snapshot_id = snapshot_id
