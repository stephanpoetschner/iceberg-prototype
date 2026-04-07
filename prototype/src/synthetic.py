import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal


@dataclass(frozen=True)
class CommunityProfile:
    num_points: int


# Class-level constants defined after the class
CommunityProfile.SMALL   = CommunityProfile(num_points=5)
CommunityProfile.AVERAGE = CommunityProfile(num_points=20)
CommunityProfile.LARGE   = CommunityProfile(num_points=50)
CommunityProfile.OUTLIER = CommunityProfile(num_points=200)

RECORD_TYPES  = [1, 2, 3, 4, 5]
VALUE_TYPES   = [1, 2, 3]
SLOTS_PER_DAY = 96    # 15-min resolution
NULL_RATE     = 0.02


def _days_in_month(period_start: datetime) -> int:
    """Return the number of days in the month starting at period_start."""
    year, month = period_start.year, period_start.month
    if month == 12:
        next_month = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        next_month = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return (next_month - period_start.replace(day=1)).days


def generate_records(
    community_id: int,
    period_start: datetime,
    num_points: int,
    seed: int = 42,
) -> list[dict]:
    """Generate deterministic synthetic billing records for one community-month.

    Returns a list of dicts matching ARROW_SCHEMA column names (minus snapshot_id,
    exported_at, snapshot_version — those are added by the writer).
    """
    period_start = period_start.replace(hour=0, minute=0, second=0, microsecond=0)
    rng = random.Random(hash((seed, community_id)))
    days = _days_in_month(period_start)
    slot_delta = timedelta(minutes=15)
    now_fixed = datetime(2026, 3, 26, 0, 0, 0, tzinfo=timezone.utc)  # fixed for determinism

    # Stable registration/metering IDs derived from community + point index
    registration_ids = [community_id * 1000 + i for i in range(num_points)]
    metering_ids     = [community_id * 10000 + i for i in range(num_points)]

    records = []
    for day in range(days):
        for slot in range(SLOTS_PER_DAY):
            slot_time = period_start + timedelta(days=day, minutes=slot * 15)
            for point_idx in range(num_points):
                for record_type in RECORD_TYPES:
                    if rng.random() < NULL_RATE:
                        value = None
                    else:
                        raw = rng.uniform(0.0, 50.0)
                        value = Decimal(f"{raw:.6f}")

                    records.append({
                        "community_id":       community_id,
                        "time":               slot_time,
                        "record_type":        record_type,
                        "ec_registration_id": registration_ids[point_idx],
                        "metering_point_id":  metering_ids[point_idx],
                        "value":              value,
                        "value_type":         VALUE_TYPES[record_type % len(VALUE_TYPES)],
                        "message_created_at": now_fixed,
                    })
    return records


def generate_records_iter(
    community_id: int,
    period_start: datetime,
    num_points: int,
    seed: int = 42,
):
    """Generator variant of generate_records. Yields one record dict at a time.

    Use this in benchmarks and production code to avoid materialising the full
    record list in memory. Produces identical records in identical order to
    generate_records() with the same arguments.
    """
    period_start = period_start.replace(hour=0, minute=0, second=0, microsecond=0)
    rng = random.Random(hash((seed, community_id)))
    days = _days_in_month(period_start)
    now_fixed = datetime(2026, 3, 26, 0, 0, 0, tzinfo=timezone.utc)

    registration_ids = [community_id * 1000 + i for i in range(num_points)]
    metering_ids     = [community_id * 10000 + i for i in range(num_points)]

    for day in range(days):
        for slot in range(SLOTS_PER_DAY):
            slot_time = period_start + timedelta(days=day, minutes=slot * 15)
            for point_idx in range(num_points):
                for record_type in RECORD_TYPES:
                    if rng.random() < NULL_RATE:
                        value = None
                    else:
                        raw = rng.uniform(0.0, 50.0)
                        value = Decimal(f"{raw:.6f}")

                    yield {
                        "community_id":       community_id,
                        "time":               slot_time,
                        "record_type":        record_type,
                        "ec_registration_id": registration_ids[point_idx],
                        "metering_point_id":  metering_ids[point_idx],
                        "value":              value,
                        "value_type":         VALUE_TYPES[record_type % len(VALUE_TYPES)],
                        "message_created_at": now_fixed,
                    }
