# prototype/tests/test_dst.py
"""
DST boundary correctness for SNAP/NON-SNAP billing queries.

SNAP window = 10:00–16:00 Vienna local time (Europe/Vienna).
Spring forward (2026-03-29): hour 02:00–03:00 skipped (no slots should appear there).
Fall back   (2026-10-25): hour 02:00–03:00 occurs twice (wall clock ambiguity).
"""
from datetime import datetime, timezone
from decimal import Decimal

import duckdb
import pyarrow as pa
import pytest

from schema import ARROW_SCHEMA
from snapshot_id import make_snapshot_id
from writer import write_community_snapshot

EXPORTED = datetime(2026, 3, 26, 12, 0, 0, tzinfo=timezone.utc)


def _make_records(community_id: int, slots: list[datetime], value: Decimal = Decimal("1.000000")) -> list[dict]:
    """Build minimal records for given UTC slot timestamps."""
    return [
        {
            "community_id":       community_id,
            "time":               slot,
            "record_type":        1,
            "ec_registration_id": community_id * 1000,
            "metering_point_id":  community_id * 10000,
            "value":              value,
            "value_type":         1,
            "message_created_at": EXPORTED,
        }
        for slot in slots
    ]


def test_spring_forward_snap_classification(iceberg_table):
    """
    2026-03-29 DST: spring forward at 02:00 CET (= 01:00 UTC).
    Slots in UTC that map to Vienna local time around the transition:

      00:45 UTC = 01:45 CET  → NON-SNAP
      01:00 UTC = 02:00 CET  → skipped wall-clock hour (UTC slot exists, maps past the gap)
      02:00 UTC = 04:00 CEST → NON-SNAP  (after spring-forward)
      08:00 UTC = 10:00 CEST → first SNAP slot
      13:45 UTC = 15:45 CEST → last SNAP slot (SNAP ends at 16:00)
      14:00 UTC = 16:00 CEST → NON-SNAP
    """
    snap_slots = [
        datetime(2026, 3, 29,  8,  0, tzinfo=timezone.utc),   # 10:00 CEST
        datetime(2026, 3, 29, 13, 45, tzinfo=timezone.utc),   # 15:45 CEST (last SNAP)
    ]
    nonsnap_slots = [
        datetime(2026, 3, 29,  0, 45, tzinfo=timezone.utc),   # 01:45 CET (NON-SNAP)
        datetime(2026, 3, 29, 14,  0, tzinfo=timezone.utc),   # 16:00 CEST (NON-SNAP)
    ]
    all_slots = snap_slots + nonsnap_slots

    community_id = 70
    period       = datetime(2026, 3, 1, tzinfo=timezone.utc)
    snapshot_id  = make_snapshot_id(community_id, period, 1)
    records      = _make_records(community_id, all_slots, value=Decimal("1.000000"))
    uri, _       = write_community_snapshot(iceberg_table, records, snapshot_id=snapshot_id, exported_at=EXPORTED)

    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    row = conn.execute(
        f"""
        SELECT
            SUM(value) FILTER (
                WHERE date_part('hour', timezone('Europe/Vienna', time)) >= 10
                  AND date_part('hour', timezone('Europe/Vienna', time)) < 16
            ) AS sum_SNAP,
            SUM(value) FILTER (
                WHERE date_part('hour', timezone('Europe/Vienna', time)) < 10
                   OR date_part('hour', timezone('Europe/Vienna', time)) >= 16
            ) AS sum_NONSNAP
        FROM iceberg_scan('{uri}')
        WHERE community_id = {community_id} AND snapshot_id = {snapshot_id}
        """
    ).fetchone()

    sum_snap, sum_nonsnap = row
    # 2 SNAP slots × 1.0 = 2.0; 2 NON-SNAP slots × 1.0 = 2.0
    assert sum_snap    == Decimal("2.000000"), f"Expected SNAP=2.0, got {sum_snap}"
    assert sum_nonsnap == Decimal("2.000000"), f"Expected NON-SNAP=2.0, got {sum_nonsnap}"


def test_fall_back_snap_classification(iceberg_table):
    """
    2026-10-25 DST: fall back at 03:00 CEST (= 01:00 UTC).
    SNAP hours are 10:00–16:00 Vienna local time.
    The DST transition occurs at 01:00 UTC, so all slots after 01:00 UTC are CET (+1).
    After fall-back, SNAP in UTC = 09:00–15:00 (CET +1).
    DuckDB's timezone() handles this correctly by using the UTC timestamp directly.
    """
    snap_slots = [
        datetime(2026, 10, 25,  9,  0, tzinfo=timezone.utc),  # 10:00 CET  → SNAP
        datetime(2026, 10, 25, 13, 45, tzinfo=timezone.utc),  # 14:45 CET  → SNAP
        datetime(2026, 10, 25, 14, 45, tzinfo=timezone.utc),  # 15:45 CET  → SNAP
    ]
    nonsnap_slots = [
        datetime(2026, 10, 25,  8,  0, tzinfo=timezone.utc),  # 09:00 CET  → NON-SNAP
        datetime(2026, 10, 25, 15,  0, tzinfo=timezone.utc),  # 16:00 CET  → NON-SNAP
    ]
    all_slots = snap_slots + nonsnap_slots

    community_id = 71
    period       = datetime(2026, 10, 1, tzinfo=timezone.utc)
    snapshot_id  = make_snapshot_id(community_id, period, 1)
    records      = _make_records(community_id, all_slots, value=Decimal("1.000000"))
    uri, _       = write_community_snapshot(iceberg_table, records, snapshot_id=snapshot_id, exported_at=EXPORTED)

    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    row = conn.execute(
        f"""
        SELECT
            SUM(value) FILTER (
                WHERE date_part('hour', timezone('Europe/Vienna', time)) >= 10
                  AND date_part('hour', timezone('Europe/Vienna', time)) < 16
            ) AS sum_SNAP,
            SUM(value) FILTER (
                WHERE date_part('hour', timezone('Europe/Vienna', time)) < 10
                   OR date_part('hour', timezone('Europe/Vienna', time)) >= 16
            ) AS sum_NONSNAP
        FROM iceberg_scan('{uri}')
        WHERE community_id = {community_id} AND snapshot_id = {snapshot_id}
        """
    ).fetchone()

    sum_snap, sum_nonsnap = row
    # 3 SNAP slots × 1.0 = 3.0; 2 NON-SNAP slots × 1.0 = 2.0
    assert sum_snap    == Decimal("3.000000"), f"Expected SNAP=3.0, got {sum_snap}"
    assert sum_nonsnap == Decimal("2.000000"), f"Expected NON-SNAP=2.0, got {sum_nonsnap}"


def test_snap_lower_boundary_is_inclusive(iceberg_table):
    """Slot exactly at 10:00 Vienna local time must be classified as SNAP."""
    # 2026-01-15 is standard CET (+1). SNAP starts at 10:00 CET = 09:00 UTC.
    snap_boundary_slot   = datetime(2026, 1, 15,  9,  0, tzinfo=timezone.utc)  # 10:00 CET → SNAP
    nonsnap_just_before  = datetime(2026, 1, 15,  8, 45, tzinfo=timezone.utc)  # 09:45 CET → NON-SNAP

    community_id = 80
    period       = datetime(2026, 1, 1, tzinfo=timezone.utc)
    snapshot_id  = make_snapshot_id(community_id, period, 1)
    records      = _make_records(community_id, [snap_boundary_slot, nonsnap_just_before], value=Decimal("1.000000"))
    uri, _       = write_community_snapshot(iceberg_table, records, snapshot_id=snapshot_id, exported_at=EXPORTED)

    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    row = conn.execute(
        f"""
        SELECT
            SUM(value) FILTER (
                WHERE date_part('hour', timezone('Europe/Vienna', time)) >= 10
                  AND date_part('hour', timezone('Europe/Vienna', time)) < 16
            ) AS sum_SNAP,
            SUM(value) FILTER (
                WHERE date_part('hour', timezone('Europe/Vienna', time)) < 10
                   OR date_part('hour', timezone('Europe/Vienna', time)) >= 16
            ) AS sum_NONSNAP
        FROM iceberg_scan('{uri}')
        WHERE community_id = {community_id} AND snapshot_id = {snapshot_id}
        """
    ).fetchone()
    sum_snap, sum_nonsnap = row
    assert sum_snap    == Decimal("1.000000"), f"10:00 CET must be SNAP, got sum_SNAP={sum_snap}"
    assert sum_nonsnap == Decimal("1.000000"), f"09:45 CET must be NON-SNAP, got sum_NONSNAP={sum_nonsnap}"


def test_snap_upper_boundary_is_exclusive(iceberg_table):
    """Slot exactly at 16:00 Vienna local time must be classified as NON-SNAP."""
    # 2026-01-15 CET (+1). SNAP ends before 16:00 CET = 15:00 UTC.
    snap_last_slot   = datetime(2026, 1, 15, 14, 45, tzinfo=timezone.utc)  # 15:45 CET → SNAP
    nonsnap_boundary = datetime(2026, 1, 15, 15,  0, tzinfo=timezone.utc)  # 16:00 CET → NON-SNAP

    community_id = 81
    period       = datetime(2026, 1, 1, tzinfo=timezone.utc)
    snapshot_id  = make_snapshot_id(community_id, period, 1)
    records      = _make_records(community_id, [snap_last_slot, nonsnap_boundary], value=Decimal("1.000000"))
    uri, _       = write_community_snapshot(iceberg_table, records, snapshot_id=snapshot_id, exported_at=EXPORTED)

    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    row = conn.execute(
        f"""
        SELECT
            SUM(value) FILTER (
                WHERE date_part('hour', timezone('Europe/Vienna', time)) >= 10
                  AND date_part('hour', timezone('Europe/Vienna', time)) < 16
            ) AS sum_SNAP,
            SUM(value) FILTER (
                WHERE date_part('hour', timezone('Europe/Vienna', time)) < 10
                   OR date_part('hour', timezone('Europe/Vienna', time)) >= 16
            ) AS sum_NONSNAP
        FROM iceberg_scan('{uri}')
        WHERE community_id = {community_id} AND snapshot_id = {snapshot_id}
        """
    ).fetchone()
    sum_snap, sum_nonsnap = row
    assert sum_snap    == Decimal("1.000000"), f"15:45 CET must be SNAP, got sum_SNAP={sum_snap}"
    assert sum_nonsnap == Decimal("1.000000"), f"16:00 CET must be NON-SNAP, got sum_NONSNAP={sum_nonsnap}"
