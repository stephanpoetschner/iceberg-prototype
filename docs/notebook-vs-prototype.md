# Approach Comparison: Prototype vs. Notebook

**Date**: 2026-03-27

Comparing `prototype/src/` (modular, tested Python package) against `340-iceberg-s3.ipynb` (exploratory spike against real production data via Django ORM and a real S3 bucket).

These are not really competing designs — the notebook is a spike that proved S3 + DuckDB works end-to-end. But several concrete choices in the notebook would cause real problems if promoted to production, and a few things the notebook discovered are genuinely valuable.

---

## Where the Notebook Has an Edge

**1. Real data, real S3, real credentials**

The notebook queries actual `Record` objects via the Django ORM and writes to S3. This exercised real-world data quality (nulls, timezone offsets, distribution of records per registration) that synthetic data can only approximate. When a bug exists in production data — e.g. sparse registrations, fractional quarter-hours — the notebook finds it; the prototype won't.

**2. DuckDB + S3 auth configuration**

The notebook worked through three different S3 credential mechanisms (`SET s3_access_key_id`, `CREATE SECRET`, env vars) before landing on `CREATE SECRET`. That's genuinely useful operational knowledge. The prototype avoids S3 entirely and will need to rediscover this.

**3. Quick feedback loop**

Notebooks are fast for confirming "does this query shape return the right thing?" The iterative cell structure (Cell 20: basic scan; Cell 21: SNAP/NONSNAP query) is appropriate for exploration. The prototype's test suite runs in seconds too, but building a test case requires more ceremony.

---

## Where the Notebook Falls Short

**1. Partitioning by `snapshot_id` is wrong and will blow up**

```python
# notebook
PartitionField(source_id=1, field_id=1, transform=IdentityTransform(), name="snapshot_id")
```

Every snapshot gets its own partition directory on S3. At 2,500 communities × 12 months × any correction versions, you accumulate tens of thousands of snapshot-specific directories. Iceberg metadata operations (listing, planning) slow down proportionally — AWS S3 LIST requests scale linearly with partition count.

The prototype uses a **month transform on `time`**:

```python
# prototype schema.py
PartitionSpec(
    PartitionField(source_id=2, field_id=2, transform=IdentityTransform(), name="community_id"),
    PartitionField(source_id=3, field_id=1003, transform=MonthTransform(), name="time_month"),
)
```

This caps growth at ~30K partitions total (2,500 × 12), regardless of how many correction snapshots are written.

**2. `uint8` for `community_id` overflows at 256 — you have 2,500 communities**

```python
# notebook
pyarrow.field("community_id", pyarrow.uint8(), nullable=False),  # max 255
```

This silently wraps around. Community 256 becomes community 0. Queries would return mixed results from multiple real communities. The prototype uses `int32` (max ~2 billion).

**3. Timestamps are timezone-naive — SNAP/NONSNAP query is wrong**

```python
# notebook: TimestampType() — no timezone
NestedField(field_id=3, name="time", field_type=TimestampType(), required=True),
```

The prototype uses `TimestamptzType()`. This matters in Austria because Vienna switches between UTC+1 (winter) and UTC+2 (summer). The SNAP window is 10:00–16:00 **Vienna time**.

In the notebook's query:

```sql
-- notebook
SUM(value) FILTER (WHERE date_part('hour', time) >= 10 AND date_part('hour', time) < 16) AS sum_SNAP
```

On UTC timestamps without timezone conversion, the SNAP window shifts by 1–2 hours depending on DST. In winter, records from 09:00–15:00 UTC map to 10:00–16:00 Vienna, so the filter boundary is off by an hour. Billing amounts would be wrong on DST transition days.

The prototype handles this correctly:

```sql
-- prototype reader.py
timezone('Europe/Vienna', time) >= TIME '10:00' AND timezone('Europe/Vienna', time) < TIME '16:00'
```

**4. Random snapshot IDs — no idempotency**

```python
# notebook
SNAPSHOT_ID = random.randrange(2**32)
```

If a Celery task crashes halfway through and retries, it generates a new random ID and appends a second copy of the data. The next query doubles every value. There's no way to detect or prevent this.

The prototype derives a deterministic ID from `(community_id, period_start, snapshot_version)` via SHA-256. Retry → same ID → pre-write scan detects the duplicate → raises `SnapshotAlreadyExistsError`.

**5. SQLite catalog won't handle concurrent writes**

```python
# notebook
"uri": f"sqlite:///{catalog_path}/pyiceberg_catalog.db",
```

SQLite uses file-level locking. Two Celery workers writing different communities at the same time will serialize or deadlock on the catalog. The prototype targets PostgreSQL (matching production), which supports row-level locking and concurrent OCC commits.

**6. `uint64` / signed int64 type mismatch**

The notebook's Arrow schema uses `uint64` for `snapshot_id` but the Iceberg schema declares `LongType()` (signed int64). PyArrow's `uint64` can represent values that don't fit in a signed int64. Random IDs from `randrange(2**32)` happen to stay in range, but this is an accident. The prototype uses `int64` throughout.

**7. Sidecar is ad-hoc and coupled to Django**

```python
# notebook
snapshot_metadata = {
    "snapshot_id": SNAPSHOT_ID,
    "community": CommunitySerializer(community).data,
    ...
}
```

This works only inside the Django process. The metadata format depends on `CommunitySerializer` — if that serializer changes (it will), historical sidecars may not parse. The prototype uses a standalone Pydantic model with explicit fields and version-stable serialization.

**8. Hard-coded credentials**

AWS credentials appear in plain text in the notebook and are presumably committed to git. Rotate immediately and use environment variables or a secrets manager.

---

## Summary Table

| Concern | Notebook | Prototype |
|---|---|---|
| Data source | Real Django ORM | Synthetic (deterministic) |
| Catalog backend | SQLite (single-writer) | PostgreSQL (concurrent) |
| Partitioning | `community_id` + `snapshot_id` — unbounded | `community_id` + `time_month` — bounded at ~30K |
| `community_id` type | `uint8` — overflows at 256 | `int32` — safe to 2 billion |
| Timestamp type | Timezone-naive | Timezone-aware (`TimestamptzType`) |
| SNAP/NONSNAP | Wrong on DST days | DST-correct via `timezone('Europe/Vienna', ...)` |
| Snapshot IDs | Random, not idempotent | Deterministic SHA-256 |
| Duplicate detection | None | Pre-write scan + `SnapshotAlreadyExistsError` |
| S3 credential config | Explored; `CREATE SECRET` works | Not yet tested |
| Test coverage | None | ~40 tests |
| Portability | Django-coupled | Standalone |

---

## What to Carry Forward from the Notebook

1. **Use `CREATE SECRET` for DuckDB S3 auth** — it works; the legacy `SET s3_access_key_id` approach and env vars are less reliable.
2. **The core assumption is validated** — PyIceberg → S3 → DuckDB without a catalog at query time works. The prototype's read path is sound.
3. **Partitioning-by-snapshot-id is an instructive failure** — it reveals the temptation to put the query key in the partition spec. Iceberg partitions are for data skipping, not lookup indexing; `snapshot_id` belongs in a `WHERE` clause, not a `PartitionField`.
