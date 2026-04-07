# src/lock.py
"""Redis-backed distributed lock for snapshot writes.

Usage:
    with snapshot_lock(redis_client, snapshot_id):
        write_community_snapshot(...)

The lock uses Redis SET NX (set-if-not-exists) with a TTL so a crashed process
cannot hold the lock indefinitely. TTL defaults to 300 seconds — longer than any
expected write duration for a 200-point outlier community.
"""
from contextlib import contextmanager

import redis


class SnapshotLockError(Exception):
    """Raised when the distributed lock for snapshot_id cannot be acquired.

    This means another process is currently writing this snapshot_id. The caller
    should retry after a short delay, or check whether the snapshot was already
    committed (which would raise SnapshotAlreadyExistsError instead).
    """
    def __init__(self, snapshot_id: int):
        super().__init__(
            f"snapshot_id={snapshot_id} is locked by another writer — retry after delay"
        )
        self.snapshot_id = snapshot_id


@contextmanager
def snapshot_lock(redis_client: redis.Redis, snapshot_id: int, ttl_seconds: int = 300):
    """Acquire an exclusive lock for snapshot_id. Raises SnapshotLockError if held.

    The lock is automatically released when the context manager exits, even on
    exception. The TTL ensures the lock expires if the process crashes.
    """
    key = f"billing_archive:snapshot_lock:{snapshot_id}"
    acquired = redis_client.set(key, b"1", nx=True, ex=ttl_seconds)
    if not acquired:
        raise SnapshotLockError(snapshot_id)
    try:
        yield
    finally:
        redis_client.delete(key)
