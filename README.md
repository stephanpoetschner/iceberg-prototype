# Billing Archive Specification

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
| Metering points | 50,000 | Given |
| Resolution | 15 min → 96 slots/day/point | Given |
| Record types per slot | ~5 | Observed: community 127 has 238,080 records / 16 points / 2,976 slots ≈ 5 |
| Records per metering point per month | ~14,880 | 2,976 slots × 5 record types |
| Records per community per month (avg 20 points) | ~297,600 | 20 × 14,880 |
| Total records per month (all communities) | ~744M | 50,000 × 14,880 |
| Compressed size per community per month | ~280 KB | Observed ~225 KB for 16-point community; scaled for 20 |
| Total compressed size per month | ~700 MB | 2,500 communities × 280 KB |
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
        {snapshot_id}.metadata.json.sha256
```

The `iceberg-warehouse/` tree is owned by PyIceberg — do not write into it manually.
The `snapshots/` tree is owned by our pipeline — structured, predictable, independent of Iceberg internals.

### 4.2 Catalog

Use **PostgreSQL** (existing application DB) as the Iceberg SQL catalog.

**Why not SQLite on S3**: S3 has no atomic file update semantics. Two concurrent Celery workers
writing to the same SQLite file on S3 will corrupt it. This is not a theoretical risk — it will
happen when two communities are exported in parallel.

**Why not local SQLite**: The catalog must survive server restarts and deployments. A local file
does not.

**Why PostgreSQL over a dedicated catalog service** (Hive Metastore, AWS Glue, Polaris): our team
is ~5 people. PostgreSQL is already operated, backed up, and understood. PyIceberg supports it
natively. Add complexity only when outgrown.

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

**Changes from prototype and rationale:**

| Field | Prototype | Spec | Why |
|---|---|---|---|
| `time` | `TimestampType()` (no tz) | `TimestamptzType()` (UTC+offset) | DST transitions affect SNAP window (10–16h local time). 2 days/year are wrong with naive timestamps. |
| `message_created_at` | `TimestampType()` | `TimestamptzType()` | Consistency; avoids silent offset loss. |
| `value` | `required=True` | `required=False` | Missing meter readings are a real domain event. Forcing non-null means the pipeline either crashes or silently substitutes a value. |
| `exported_at` | absent | `TimestamptzType()` required | Records when this row entered the archive. Essential for audit: "was this record available at the time the invoice was issued?" |
| `community_id` | `IntegerType()` in Iceberg, `uint8` in PyArrow | `IntegerType()` everywhere | The prototype has a type mismatch. Iceberg schema is the authority; PyArrow schema must match exactly. |
| `snapshot_id` | `LongType()` in Iceberg, `uint64` in PyArrow | `LongType()` everywhere | Same mismatch. High-bit values coerce incorrectly between signed and unsigned 64-bit. |
| `snapshot_version` | absent | `IntegerType()` required (field 11) | Mentioned in §7.1 text but was missing from schema. Parquet files must be self-describing — a reader with only data files needs to identify the version. |

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

### 6.1 Recommended Partition Spec

```python
from pyiceberg.transforms import IdentityTransform, MonthTransform

partition_spec = PartitionSpec(
    PartitionField(source_id=2, field_id=100, transform=IdentityTransform(), name="community_id"),
    PartitionField(source_id=3, field_id=101, transform=MonthTransform(),    name="time_month"),
)
```

### 6.2 Why Not `(community_id, snapshot_id)` (prototype approach)

`snapshot_id` is an application concept — one per billing period per community per export run.
Using it as a partition key means **every export creates new partition directories forever**:

| Scenario | Partitions after 1 year | Partitions after 5 years |
|---|---|---|
| `(community_id, snapshot_id)`, 1 export/month | 2,500 × 12 = **30,000** | 150,000 |
| `(community_id, snapshot_id)`, 2 exports/month (corrections) | 2,500 × 24 = **60,000** | 300,000 |
| `(community_id, month(time))` — recommended | 2,500 × 12 = **30,000** | 150,000 |
| `(community_id, month(time))`, 2 exports/month | still **30,000** | still 150,000 |

With the prototype partitioning, re-exports due to late corrections double (or triple) the partition
count. Iceberg metadata scanning degrades with partition count — above ~100k partitions you need
a dedicated catalog service. With `month(time)`, re-exports append files to the same partition;
Iceberg handles this efficiently and partition count stays bounded.

### 6.3 Why Not Hash Bucketing on `community_id`

Hash bucketing (`sha256(community_id)[:2]` or Iceberg's `BucketTransform(N)`) is designed to
avoid S3 hot-partition throttling from alphabetical key prefixes. **AWS resolved this in 2018** —
S3 now auto-scales per prefix regardless of naming. Hash bucketing on `community_id` would mix
multiple communities' data into the same Parquet files, forcing full-file scans whenever you
filter by a single community. The access pattern is always per-community, so this is strictly
worse.

### 6.4 Why `month(time)` Instead of `identity(time)`

`identity(time)` on a timestamp with 15-minute resolution would create one partition per
15-minute slot — ~2,976 partitions/community/month. `MonthTransform` groups an entire month
into one partition, matching the billing period exactly.

### 6.5 File Sizes and the Small File Reality

Parquet files at this partitioning level are small:

| Community size | Records/month | Compressed file size |
|---|---|---|
| 16 points (observed) | 238,080 | ~225 KB |
| 20 points (average) | 297,600 | ~280 KB |
| 50 points | 744,000 | ~700 KB |
| 200 points (large) | 2,976,000 | ~2.8 MB |

The analytical sweet spot for Parquet is 128 MB – 1 GB per file. The average community
partition is ~450× smaller than optimal. **This is not a problem for per-community queries**
(the primary use case) — a `WHERE community_id = X AND snapshot_id = Y` query opens one
file, which reads in milliseconds over S3 regardless of size.

It becomes a problem for **cross-community queries** (e.g. "sum all communities for January"):
that opens 2,500 files, each requiring a separate S3 HTTP request (~10–50 ms each) — 25–125
seconds of pure I/O before reading any data. If this use case arises, the solution is
periodic compaction via Iceberg's `rewrite_data_files()`, not a repartitioning.

### 6.6 Mandatory `snapshot_id` Filter

With `(community_id, month(time))` partitioning, multiple snapshot versions for the same
period land in the **same partition directory**. A scan without a `snapshot_id` filter
returns rows from all versions silently mixed together — wrong results, no error.

**Every query against this table must include `WHERE snapshot_id = X`.** Document this
explicitly when handing credentials and the `METADATA_URI` to downstream teams.

---

## 7. Snapshot Identity

### 7.1 Deterministic Snapshot ID

The prototype uses `random.randrange(2**32)` — a 32-bit random integer. Problems:

- **Collision risk**: birthday paradox gives ~50% collision probability at 65,000 snapshots
  (~2.6 years at 2,500 communities/month, 1 export/month).
- **No idempotency**: re-running the notebook creates a new snapshot for the same period,
  silently duplicating data.
- **Not debuggable**: log messages with ID `3928349580` carry no human context.

**Use a deterministic ID derived from the inputs:**

```python
import hashlib

def make_snapshot_id(community_id: int, period_start: datetime, snapshot_version: int = 1) -> int:
    key = f"{community_id}:{period_start.isoformat()}:{snapshot_version}"
    return int(hashlib.sha256(key.encode()).hexdigest()[:16], 16) & 0x7FFFFFFFFFFFFFFF
    # masked to signed int64 range to match Iceberg LongType
```

Same community + same period + same version → same ID. The pipeline must check existence before writing to ensure idempotency (crash-retry safety):

```python
existing = table.scan(
    row_filter=f"community_id = {community_id} AND snapshot_id = {snapshot_id}"
).to_arrow()
if len(existing) > 0:
    raise SnapshotAlreadyExistsError(snapshot_id)
```

For **corrections** (late meter data, revised values): use a separate `snapshot_version` column
(integer, default 1) and include it in the ID hash. This allows `(community_id, period, version=2)`
to coexist with `version=1` in the archive, with the downstream pipeline explicitly choosing
which version to use for invoicing.

---

## 8. Metadata Sidecar

### 8.1 Purpose

The sidecar is a **metadata and discovery document**. The Iceberg Parquet
files contain only integers and timestamps. The sidecar carries the business context needed
to interpret them: community names, metering point AT-IDs, energy direction, registration
validity periods. Without it, the Parquet files are opaque.

Cryptographic sealing (proving "this invoice was based on this exact data") is the billing
pipeline's responsibility, not the archival pipeline's. The billing pipeline seals at invoice
generation time using `snapshot_id` as the stable data reference.

### 8.2 Location

Written to S3 at a stable, predictable path — independent of Iceberg's internal metadata paths
(which change on every append):

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
    egon_id: int
    name: str                  # AT-metering point ID
    energy_direction: int      # 1=import, 2=export

class ECRegistrationRecord(BaseModel):
    id: int
    egon_id: int
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

The sidecar is a **metadata and discovery document**, not an audit seal. It carries the business
context needed to interpret the archive and the `iceberg_metadata_uri` to locate it.

**Cryptographic sealing is the billing pipeline's responsibility.** When the billing pipeline
generates an invoice, it seals the evidence: the invoice itself, the `snapshot_id` it queried,
and a hash of the data it consumed. The `snapshot_id` is the stable reference — it identifies
the same logical rows regardless of physical file layout (compaction, rewrites, etc.).

### 8.4 Retrieving `iceberg_metadata_uri`

Do not hardcode this. PyIceberg provides it after write:

```python
table.append(arrow_table)
metadata_uri = table.metadata_location   # always points to current metadata
```

---

## 9. Data Extraction

### 9.1 Problem with Current Approach

The prototype iterates the Django ORM record-by-record:

```python
for record in records:
    times.append(record.time)
    ...
```

At scale: 50,000 metering points × ~14,880 records = **744M rows/month**. Django ORM
instantiates a Python object per row. At 1µs per object (optimistic), that is 744 seconds
per full export run, plus significant memory pressure.

### 9.2 Recommended: `.values_list()` with Chunked Arrow Batches

Minimum viable improvement — single line change, 10–50x faster:

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

# chunk into Arrow batches to avoid loading all rows into memory
CHUNK_SIZE = 100_000
for chunk in chunked_queryset(records_qs, CHUNK_SIZE):
    batch = pyarrow.Table.from_pydict({...}, schema=arrow_schema)
    table.append(batch)
```

**Better option** (future): use `adbc-driver-postgresql` or `psycopg3` binary cursor to
stream Arrow record batches directly from PostgreSQL, bypassing Django ORM entirely. This
is the path for full-scale 744M row exports.

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
3. Check registry (`BillingSnapshot` model) for existing `snapshot_id`. Raise `SnapshotAlreadyExistsError` if found.
4. Query EC registrations active during the period.
5. Extract records from TimescaleDB in chunks → build a single PyArrow Table → write to Iceberg in one atomic `table.append()`.
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

**Why not query the Iceberg table for discovery**: Iceberg has no efficient way to ask
"what snapshot_ids exist for community 127?" without a full table scan. The registry
makes this O(1).

**Why not list S3 sidecar files**: S3 LIST is eventually consistent, slower than a DB
query, and requires parsing filenames to extract metadata. The registry is the right tool.

### 11.4 What Stays Out of Scope for Now

- Concurrent community exports (can be added by dispatching one task per community).

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

These need a decision before implementation:

1. ~~**Snapshot version semantics**~~ — **Resolved**: initial export is `version=1`, triggered
   automatically once completeness is confirmed. Corrections (`version=2+`) are triggered
   explicitly by an operator. The invoice pipeline always references a specific `snapshot_id`,
   never "the latest."

2. ~~**Completeness signal**~~ — **Resolved**: an existing per-metering-point status flag in
   the Django models tracks confirmation. The pipeline reads this flag. See Section 10.1 for
   the placeholder — fill in the actual model/field name before implementation.

3. ~~**DST slot count**~~ — **Resolved**: completeness is determined by the application flag,
   not slot counting. DST edge cases are handled upstream.

4. **Data retention**: Iceberg accumulates its own internal snapshot history (every `table.append()`
   call creates an Iceberg-level snapshot). These are separate from our application snapshots and
   grow indefinitely. Run `expire_snapshots()` periodically (e.g. keep 30 days of Iceberg history)
   to prevent unbounded metadata growth.

5. ~~**Parquet hash computation**~~ — **Resolved**: The archival pipeline does not hash Parquet
   files. Cryptographic sealing is the billing pipeline's responsibility. The billing pipeline
   seals at invoice generation time using `snapshot_id` as the stable data reference. This
   decouples the archive from audit concerns and allows compaction to run freely.

6. **Access control**: are all communities in one S3 bucket, or isolated per customer? This affects
   the IAM policy design for the read-only credentials handed to downstream teams.
