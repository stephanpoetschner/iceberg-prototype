# prototype/tests/test_snapshot_id.py
import pytest
from datetime import datetime, timezone
from snapshot_id import make_snapshot_id, SnapshotAlreadyExistsError


PERIOD = datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_deterministic_same_inputs_same_id():
    id1 = make_snapshot_id(127, PERIOD, 1)
    id2 = make_snapshot_id(127, PERIOD, 1)
    assert id1 == id2


def test_different_community_different_id():
    assert make_snapshot_id(127, PERIOD, 1) != make_snapshot_id(128, PERIOD, 1)


def test_different_period_different_id():
    period2 = datetime(2026, 2, 1, tzinfo=timezone.utc)
    assert make_snapshot_id(127, PERIOD, 1) != make_snapshot_id(127, period2, 1)


def test_different_version_different_id():
    assert make_snapshot_id(127, PERIOD, 1) != make_snapshot_id(127, PERIOD, 2)


def test_result_is_positive_int64():
    snapshot_id = make_snapshot_id(127, PERIOD, 1)
    assert isinstance(snapshot_id, int)
    assert 0 <= snapshot_id <= 0x7FFFFFFFFFFFFFFF


@pytest.mark.slow
def test_collision_resistance():
    """Phase 2: 1.5M IDs across communities × months × versions × years — zero collisions.

    Skipped by default (takes 10–30s). Run with: pytest -m slow
    """
    seen = set()
    for community_id in range(1, 2501):        # 2500 communities
        for month in range(1, 13):             # 12 months
            for version in range(1, 6):        # 5 versions
                for year in range(2026, 2036): # 10 years
                    period = datetime(year, month, 1, tzinfo=timezone.utc)
                    sid = make_snapshot_id(community_id, period, version)
                    assert sid not in seen, f"Collision at community={community_id} month={month} version={version} year={year}"
                    seen.add(sid)


def test_collision_resistance_small():
    """Phase 1 smoke: 3,600 IDs (100 communities × 12 months × 3 versions) — zero collisions."""
    seen = set()
    for community_id in range(1, 101):    # 100 communities
        for month in range(1, 13):        # 12 months
            for version in range(1, 4):   # 3 versions
                period = datetime(2026, month, 1, tzinfo=timezone.utc)
                sid = make_snapshot_id(community_id, period, version)
                assert sid not in seen
                seen.add(sid)


def test_naive_datetime_raises():
    from datetime import datetime
    with pytest.raises(ValueError, match="timezone-aware"):
        make_snapshot_id(1, datetime(2026, 1, 1), 1)


def test_snapshot_already_exists_error_is_exception():
    err = SnapshotAlreadyExistsError(12345)
    assert isinstance(err, Exception)
    assert "12345" in str(err)
