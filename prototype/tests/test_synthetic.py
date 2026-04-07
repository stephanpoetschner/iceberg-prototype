# prototype/tests/test_synthetic.py
from datetime import datetime, timezone
from decimal import Decimal
from synthetic import generate_records, generate_records_iter, CommunityProfile


PERIOD = datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_deterministic_with_same_seed():
    r1 = generate_records(community_id=127, period_start=PERIOD, num_points=1, seed=42)
    r2 = generate_records(community_id=127, period_start=PERIOD, num_points=1, seed=42)
    assert r1 == r2


def test_different_seed_gives_different_records():
    r1 = generate_records(community_id=127, period_start=PERIOD, num_points=1, seed=42)
    r2 = generate_records(community_id=127, period_start=PERIOD, num_points=1, seed=99)
    assert r1 != r2


def test_record_count_for_1_point():
    # 1 point × 96 slots/day × 31 days × 5 record types = 14,880
    records = generate_records(community_id=127, period_start=PERIOD, num_points=1, seed=42)
    assert len(records) == 14_880


def test_record_count_scales_with_points():
    # Validate the formula scales correctly: 2× points → 2× records
    r1 = generate_records(community_id=127, period_start=PERIOD, num_points=1, seed=42)
    r2 = generate_records(community_id=127, period_start=PERIOD, num_points=2, seed=42)
    assert len(r2) == len(r1) * 2


def test_required_keys_present():
    records = generate_records(community_id=1, period_start=PERIOD, num_points=1, seed=0)
    r = records[0]
    for key in [
        "community_id", "time", "record_type", "ec_registration_id",
        "metering_point_id", "value", "value_type", "message_created_at",
    ]:
        assert key in r, f"Missing key: {key}"


def test_community_id_is_set_correctly():
    records = generate_records(community_id=999, period_start=PERIOD, num_points=1, seed=0)
    assert all(r["community_id"] == 999 for r in records)


def test_null_rate_approximately_2_percent():
    # 2 points × 14,880 = 29,760 records — enough for stable null rate measurement
    records = generate_records(community_id=1, period_start=PERIOD, num_points=2, seed=42)
    null_count = sum(1 for r in records if r["value"] is None)
    null_rate = null_count / len(records)
    assert 0.01 <= null_rate <= 0.03, f"Null rate {null_rate:.3f} out of expected 1-3% range"


def test_value_range_when_not_null():
    records = generate_records(community_id=1, period_start=PERIOD, num_points=1, seed=42)
    non_null = [r["value"] for r in records if r["value"] is not None]
    assert all(Decimal("0") <= v <= Decimal("50") for v in non_null)


def test_value_has_6_decimal_places():
    records = generate_records(community_id=1, period_start=PERIOD, num_points=1, seed=42)
    non_null = [r["value"] for r in records if r["value"] is not None]
    assert all(v == v.quantize(Decimal("0.000001")) for v in non_null)


def test_timestamps_are_utc_and_in_period():
    records = generate_records(community_id=1, period_start=PERIOD, num_points=1, seed=0)
    period_end = datetime(2026, 2, 1, tzinfo=timezone.utc)
    for r in records:
        assert r["time"].tzinfo is not None
        assert PERIOD <= r["time"] < period_end


def test_community_profile_small():
    p = CommunityProfile.SMALL
    assert p.num_points == 5


def test_community_profile_outlier():
    p = CommunityProfile.OUTLIER
    assert p.num_points == 200


def test_generate_records_iter_matches_list():
    """generate_records_iter yields the same records in the same order as generate_records."""
    period = datetime(2026, 1, 1, tzinfo=timezone.utc)
    expected = generate_records(community_id=1, period_start=period, num_points=2, seed=99)
    actual   = list(generate_records_iter(community_id=1, period_start=period, num_points=2, seed=99))
    assert len(actual) == len(expected)
    for i, (a, e) in enumerate(zip(actual, expected)):
        assert a == e, f"Record {i} differs: {a} != {e}"
