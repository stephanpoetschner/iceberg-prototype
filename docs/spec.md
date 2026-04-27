# Billing Archive — Production Specification

## 1. Goals

- Replace ad-hoc Jupyter notebook prototype with a production-grade archival pipeline.
- Produce a **frozen, verifiable snapshot** of metering records for each billing period per community.
- Enable downstream Python/data-science teams to query that data using **DuckDB without catalog access**.
- Support **re-exports and late corrections** without losing prior snapshots.
- Enable **audit trails**: prove which exact dataset was used to produce a given invoice.
- Be **future-proof**: Parquet + Iceberg is readable by any tool in 10+ years.

## 2. Non-Goals

- Real-time / streaming writes (this is a batch export pipeline).
- Serving BI or SQL-over-JDBC consumers (DuckDB is sufficient for Python teams).
- Replacing TimescaleDB as the operational database.

---

## 3. Scale & Rough Numbers

These estimates drive all partitioning and sizing decisions below.

| Parameter | Value | Source |
|---|---|---|
| Communities | ~300 | Given |
| Metering points | 50,000 | Given |
| Resolution | 15 min → 96 slots/day/point | Given |
| Record types per slot | ~5 | Observed: community 127 has 238,080 records / 16 points / 2,976 slots ≈ 5 |
| Records per metering point per month | ~14,880 | 2,976 slots × 5 record types |
| Avg metering points per community | ~167 | 50,000 / 300 |
| Largest community | up to 4,000 points | Given (operational outlier) |
| Records per community per month (avg) | ~2.48M | 167 × 14,880 |
| Total records per month (all communities) | ~744M | 50,000 × 14,880 |
| Compressed size per community per month (avg) | ~2.3 MB | Scaled from observed ~225 KB for 16-point community |
| Total compressed size per month | ~700 MB | 50,000 × 14,880 × ~1 byte/record compressed |
| Total compressed size per year | ~8.4 GB | |

**Implication**: storage cost is small (~$0.20/month on S3 Standard). The design constraint is
**query performance and operational simplicity**, not storage.

---

## 4. Storage Layout

### 4.1 S3 Structure

```
s3://{bucket}/
  iceberg-warehouse/
    billing_exports/
      records/                    ← Iceberg table (data + metadata managed by PyIceberg)
        metadata/
          ...metadata.json
          ...avro
        data/
          community_id=127/
            time_month=2026-01/
              {uuid}.parquet
  snapshots/                      ← Sidecar files (managed by our pipeline)
    {community_id}/
      {period}/                   ← e.g. "2026-01"
        {snapshot_id}.metadata.json
```

The `iceberg-warehouse/` tree is owned by PyIceberg — do not write into it manually.
The `snapshots/` tree is owned by our pipeline — structured, predictable, independent of Iceberg internals.

### 4.2 Catalog

Use **PostgreSQL** (existing application DB) as the Iceberg SQL catalog. See [Design Decisions §1](reference/design-decisions.md#1-catalog-postgresql) for the rationale.

```python
catalog = load_catalog("production", **{
    "type": "sql",
    "uri": settings.DATABASE_URL,          # reuse existing Django DB
    "warehouse": f"s3://{BUCKET}/iceberg-warehouse",
    "filesystem.s3.aws_access_key_id":     settings.AWS_ACCESS_KEY_ID,
    "filesystem.s3.aws_secret_access_key": settings.AWS_SECRET_ACCESS_KEY,
    "filesystem.s3.endpoint":              settings.AWS_S3_ENDPOINT_URL,  # if custom endpoint
})
```

---

## 5. Iceberg Table Schema

### 5.1 `billing_exports.records`

```python
from pyiceberg.types import (
    NestedField, LongType, IntegerType, TimestamptzType, DecimalType
)

schema = Schema(
    NestedField(field_id=1,  name="snapshot_id",         field_type=LongType(),           required=True),
    NestedField(field_id=2,  name="community_id",        field_type=IntegerType(),         required=True),
    NestedField(field_id=3,  name="time",                field_type=TimestamptzType(),     required=True),
    NestedField(field_id=4,  name="record_type",         field_type=IntegerType(),         required=True),
    NestedField(field_id=5,  name="ec_registration_id",  field_type=LongType(),            required=True),
    NestedField(field_id=6,  name="metering_point_id",   field_type=LongType(),            required=True),
    NestedField(field_id=7,  name="value",               field_type=DecimalType(22, 6),    required=False),  # nullable: missing readings exist
    NestedField(field_id=8,  name="value_type",          field_type=IntegerType(),         required=True),
    NestedField(field_id=9,  name="message_created_at",  field_type=TimestamptzType(),     required=True),
    NestedField(field_id=10, name="exported_at",         field_type=TimestamptzType(),     required=True),
    NestedField(field_id=11, name="snapshot_version",    field_type=IntegerType(),         required=True),  # 1=original, 2+=corrections
)
```

See [Design Decisions §12](reference/design-decisions.md#12-schema-corrections-from-the-initial-design) for why these fields differ from the initial design.

### 5.2 PyArrow Schema (must mirror Iceberg exactly)

```python
arrow_schema = pyarrow.schema([
    pyarrow.field("snapshot_id",        pyarrow.int64(),          nullable=False),
    pyarrow.field("community_id",       pyarrow.int32(),          nullable=False),
    pyarrow.field("time",               pyarrow.timestamp("us", tz="UTC"), nullable=False),
    pyarrow.field("record_type",        pyarrow.int32(),          nullable=False),
    pyarrow.field("ec_registration_id", pyarrow.int64(),          nullable=False),
    pyarrow.field("metering_point_id",  pyarrow.int64(),          nullable=False),
    pyarrow.field("value",              pyarrow.decimal128(22, 6), nullable=True),
    pyarrow.field("value_type",         pyarrow.int32(),          nullable=False),
    pyarrow.field("message_created_at", pyarrow.timestamp("us", tz="UTC"), nullable=False),
    pyarrow.field("exported_at",        pyarrow.timestamp("us", tz="UTC"), nullable=False),
    pyarrow.field("snapshot_version",  pyarrow.int32(),                    nullable=False),
])
```

Note: `timestamp("us", tz="UTC")` maps to `TimestamptzType()` in Iceberg. Microsecond precision
is standard; second precision (`"s"`) loses sub-second timestamps present in `message_created_at`.

---

## 6. Partitioning

```python
from pyiceberg.transforms import IdentityTransform, MonthTransform

partition_spec = PartitionSpec(
    PartitionField(source_id=2, field_id=100, transform=IdentityTransform(), name="community_id"),
    PartitionField(source_id=3, field_id=101, transform=MonthTransform(),    name="time_month"),
)
```

See [Design Decisions §2–5](reference/design-decisions.md#2-partitioning-community_id-monthtime-over-community_id-snapshot_id) for the rationale behind this choice.

**Every query must include `WHERE snapshot_id = X`.** With `(community_id, month(time))` partitioning, multiple snapshot versions for the same period land in the same partition directory. A scan without a `snapshot_id` filter returns rows from all versions silently mixed together — wrong results, no error. See [Design Decisions §6](reference/design-decisions.md#6-mandatory-snapshot_id-filter).

---

## 7. Snapshot Identity

### 7.1 Deterministic Snapshot ID

```python
import hashlib

def make_snapshot_id(community_id: int, period_start: datetime, snapshot_version: int = 1) -> int:
    key = f"{community_id}:{period_start.isoformat()}:{snapshot_version}"
    return int(hashlib.sha256(key.encode()).hexdigest()[:16], 16) & 0x7FFFFFFFFFFFFFFF
    # masked to signed int64 range to match Iceberg LongType
```

Same community + same period + same version → same ID. See [Design Decisions §7](reference/design-decisions.md#7-snapshot-id-deterministic-hash-over-random) for why random IDs were rejected.

The pipeline checks existence before writing to ensure idempotency (crash-retry safety):

```python
from pyiceberg.expressions import EqualTo

existing = table.scan(row_filter=EqualTo("snapshot_id", snapshot_id)).to_arrow()
if len(existing) > 0:
    raise SnapshotAlreadyExistsError(snapshot_id)
```

This scan is the **authoritative** duplicate-commit guard. The `BillingSnapshot`
registry check in §11.2 is an optional fast path; it is advisory, while the
Iceberg scan is the one that prevents double-writes even across processes.

For **corrections** (late meter data, revised values): use `snapshot_version` (integer, default 1) included in the ID hash. This allows `(community_id, period, version=2)` to coexist with `version=1` in the archive, with the downstream pipeline explicitly choosing which version to use for invoicing.

See [Design Decisions §9](reference/design-decisions.md#9-idempotency-two-level-guard) for the full two-level idempotency model including crash recovery.

---

## 8. Metadata Sidecar

### 8.1 Purpose

The sidecar is a **metadata and discovery document**. The Iceberg Parquet files contain only integers and timestamps. The sidecar carries the business context needed to interpret them: community names, metering point IDs, energy direction, registration validity periods.

Cryptographic sealing is the billing pipeline's responsibility, not the archival pipeline's. See [Design Decisions §10](reference/design-decisions.md#10-sidecar-discovery-document-not-audit-seal).

The downstream contract — how the billing pipeline is notified of a new sidecar and how it reads the snapshot — is specified separately: [`docs/superpowers/specs/2026-04-24-billing-pipeline-interface-design.md`](superpowers/specs/2026-04-24-billing-pipeline-interface-design.md).

### 8.2 Location

Written to S3 at a stable, predictable path — independent of Iceberg's internal metadata paths (which change on every append):

```
s3://{bucket}/snapshots/{community_id}/{period}/{snapshot_id}.metadata.json
```

The sidecar is written by the pipeline **after** the Iceberg write completes, so it can include
the final `iceberg_metadata_uri`.

### 8.3 Sidecar Schema (Pydantic)

```python
from pydantic import BaseModel
from datetime import datetime

class MeteringPointRecord(BaseModel):
    id: int
    external_id: str           # third-party system identifier (opaque string)
    name: str                  # AT-metering point ID (33-char Austrian identifier)
    energy_direction: int      # 1=import, 2=export

class ECRegistrationRecord(BaseModel):
    id: int
    external_id: str           # third-party system identifier (opaque string)
    meteringpoint_id: int
    community_id: int
    registered_from: datetime
    registered_until: datetime | None

class BillingSnapshotMetadata(BaseModel):
    snapshot_id: int
    snapshot_version: int               # 1 for original, 2+ for corrections
    community_id: int
    community_name: str
    community_ec_id: str
    period_start: datetime
    period_end: datetime
    record_count: int
    exported_at: datetime
    iceberg_metadata_uri: str           # e.g. s3://.../metadata/00003-....metadata.json
    ec_registrations: list[ECRegistrationRecord]
    metering_points: list[MeteringPointRecord]
```

### 8.4 Retrieving `iceberg_metadata_uri`

Do not hardcode this. PyIceberg provides it after write:

```python
table.append(arrow_table)
metadata_uri = table.metadata_location   # always points to current metadata
```

---

## 9. Data Extraction

Use `.values_list()` with chunked PyArrow batches. See [Design Decisions §11](reference/design-decisions.md#11-data-extraction-values_list--chunked-arrow-over-orm-iteration) for why ORM iteration does not scale.

```python
records_qs = (
    Record.objects
    .filter(ec_registration_id__in=registration_ids)
    .time_slice(starts_at, ends_at)
    .order_by("recordtype", "time")
    .values_list(
        "time", "recordtype", "ec_registration_id",
        "meteringpoint_id", "value", "value_type", "eda_message_created_at"
    )
)

CHUNK_SIZE = 100_000
for chunk in chunked_queryset(records_qs, CHUNK_SIZE):
    batch = pyarrow.Table.from_pydict({...}, schema=arrow_schema)
    table.append(batch)
```

**Better option** (future): use `adbc-driver-postgresql` or `psycopg3` binary cursor to
stream Arrow record batches directly from PostgreSQL, bypassing Django ORM entirely.

---

## 10. Completeness Gate

**Archiving only happens when a full month of data is confirmed for every metering point
in the community.** Partial months are never written to the archive.

### 10.1 What "Complete" Means

A community's billing period is complete when the **existing per-metering-point status flag
in the Django models is set to confirmed** for all active EC registrations in that period.

The archival pipeline does not count slots or derive completeness independently — it reads
the application flag. This keeps completeness logic in one place (the application), and
means DST edge cases (March/October ±4 slots) are already handled upstream.

```python
def is_period_complete(community_id: int, period_start: datetime, period_end: datetime) -> bool:
    registrations = ECRegistration.objects.only_overlapping_range(period_start, period_end).filter(
        community_id=community_id
    )
    # TODO: replace with actual model/flag name
    return not registrations.filter(data_confirmed=False).exists()
```

Fill in the actual model field name before implementation.

### 10.2 Correction Exports (snapshot_version > 1)

Late meter corrections arrive after the initial archive. The policy is:

- **Initial archive** (`snapshot_version=1`): triggered automatically once completeness is
  confirmed for a period.
- **Correction archive** (`snapshot_version=2, 3, ...`): triggered **explicitly by an operator**
  (or a defined business event) — never automatically. The invoice pipeline must explicitly
  select which version to use; it does not silently pick up a newer version.

This means the downstream invoice pipeline always references a specific `snapshot_id`, not
"the latest for community X in period Y". The sidecar makes the version visible in a
human-readable field (`snapshot_version`).

---

## 11. Pipeline Design

### 11.1 Celery Task Interface

```python
@shared_task(bind=True, max_retries=3)
def export_billing_snapshot(
    self,
    community_id: int,
    period_start: str,    # ISO format: "2026-01-01T00:00:00+01:00"
    period_end: str,
    snapshot_version: int = 1,
) -> dict:
    """
    Returns: {"snapshot_id": int, "record_count": int, "sidecar_s3_key": str}
    Raises: PeriodNotCompleteError   if completeness check fails (version=1 only).
    Raises: SnapshotAlreadyExistsError if snapshot_id already present in table.
    """
```

### 11.2 Task Steps (in order)

1. If `snapshot_version == 1`: run the completeness check (Section 10.1). Raise
   `PeriodNotCompleteError` if not all metering points have full data. Do **not** retry
   automatically — this is a data availability issue, not a transient failure.
2. Compute deterministic `snapshot_id` from `(community_id, period_start, snapshot_version)`.
3. Fast-path check: query the `BillingSnapshot` registry for `snapshot_id`. If present, short-circuit and return the stored result — no Iceberg work needed. (This is a cheap optimisation; the authoritative guard lives inside `write_community_snapshot()` and runs unconditionally in step 5.)
4. Query EC registrations active during the period.
5. Extract records from TimescaleDB in chunks → build a single PyArrow Table → write to Iceberg in one atomic `table.append()`. See [Design Decisions §8](reference/design-decisions.md#8-write-atomicity-single-tableappend-per-community-export) for why this is a single commit, and the memory trade-off for large communities.
6. Capture `table.metadata_location`.
7. Build `BillingSnapshotMetadata` (including `snapshot_version`, `record_count`) and write to S3 (`snapshots/` prefix).
8. Write `BillingSnapshot` registry row in PostgreSQL (see Section 11.3).
9. Return result dict. Log snapshot_id, record_count, S3 paths.

### 11.3 Snapshot Registry (Django Model)

The registry is the primary lookup mechanism. The Celery task writes one row per successful
export. The invoice pipeline reads from it — never from Iceberg directly for discovery.

```python
class BillingSnapshot(models.Model):
    community      = models.ForeignKey("Community", on_delete=models.PROTECT)
    period_start   = models.DateTimeField()
    period_end     = models.DateTimeField()
    snapshot_version = models.PositiveSmallIntegerField(default=1)
    snapshot_id    = models.BigIntegerField(unique=True)
    record_count   = models.PositiveIntegerField()
    exported_at    = models.DateTimeField()
    iceberg_metadata_uri = models.TextField()   # s3://...metadata.json
    sidecar_s3_key = models.TextField()         # s3://...metadata-snapshot.json

    class Meta:
        unique_together = [("community", "period_start", "snapshot_version")]
        indexes = [
            models.Index(fields=["community", "period_start", "-snapshot_version"]),
        ]
```

**Getting the latest snapshot for a community/month** — the primary use case:

```python
snapshot = (
    BillingSnapshot.objects
    .filter(community_id=community_id, period_start=period_start)
    .order_by("-snapshot_version")
    .first()
)
# snapshot.snapshot_id      → pass to DuckDB query
# snapshot.iceberg_metadata_uri → pass to iceberg_scan()
```

One indexed query. No S3 listing, no Iceberg scan.

The registry is **producer-side only**: it is populated by the archival pipeline
and used by other services inside the same application (e.g. the billing pipeline
looking up which snapshot to invoice against). Downstream analytics consumers
never see it — they receive `iceberg_metadata_uri` + `snapshot_id` handed to
them out-of-band, as described in §12.

---

## 12. Query Interface for Downstream Teams

No catalog service required. Downstream teams receive:

1. The `iceberg_metadata_uri` from the sidecar.
2. S3 credentials (read-only, scoped to `billing_exports/` prefix).

They query with:

```python
import duckdb

con = duckdb.connect()
con.execute("INSTALL iceberg; LOAD iceberg;")
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute(f"""
    CREATE SECRET s3_creds (
        TYPE s3, KEY_ID '...', SECRET '...', REGION 'eu-north-1'
    );
""")

METADATA_URI = "s3://..."   # from sidecar
SNAPSHOT_ID  = 3928349580   # from sidecar

df = con.execute(f"""
    SELECT
        ec_registration_id,
        record_type,
        SUM(value) AS total,
        SUM(value) FILTER (
            WHERE date_part('hour', timezone('Europe/Vienna', time)) >= 10
              AND date_part('hour', timezone('Europe/Vienna', time)) < 16
        ) AS sum_SNAP,
        SUM(value) FILTER (
            WHERE date_part('hour', timezone('Europe/Vienna', time)) < 10
               OR date_part('hour', timezone('Europe/Vienna', time)) >= 16
            ) AS sum_NONSNAP
    FROM iceberg_scan('{METADATA_URI}')
    WHERE snapshot_id = {SNAPSHOT_ID}
    GROUP BY 1, 2
""").df()
```

Note the `timezone('Europe/Vienna', time)` call — required because timestamps are stored as UTC
(`TimestamptzType`), and SNAP hours are defined in local Austrian time.

---

## 13. Open Questions

1. **Iceberg-snapshot retention policy**: every `table.append()` creates an Iceberg-level
   snapshot (distinct from our application snapshots). These accumulate indefinitely unless
   `expire_snapshots()` is run. What retention window satisfies audit requirements — 30 days,
   90 days, 1 year? The tighter the window, the smaller the metadata; the longer the window,
   the more forensic time-travel remains available. Needs an operations/legal decision before
   scheduling the maintenance job.

2. **Access control**: are all communities in one S3 bucket, or isolated per customer? This affects
   the IAM policy design for the read-only credentials handed to downstream teams.
