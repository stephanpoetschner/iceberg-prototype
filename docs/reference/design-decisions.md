# Billing Archive — Design Decisions

Architectural decisions with rationale. For the production specification ("what to build") see [spec.md](../spec.md).

---

## 1. Catalog: PostgreSQL

**Decision:** Use the existing PostgreSQL application database as the Iceberg SQL catalog.

**Why not SQLite on S3:** S3 has no atomic file update semantics. Two concurrent Celery workers writing to the same SQLite file on S3 will corrupt it. This is not a theoretical risk — it will happen when two communities are exported in parallel.

**Why not local SQLite:** The catalog must survive server restarts and deployments. A local file does not.

**Why not a dedicated catalog service** (Hive Metastore, AWS Glue, Polaris): the team is ~5 people. PostgreSQL is already operated, backed up, and understood. PyIceberg supports it natively. Add complexity only when outgrown.

---

## 2. Partitioning: `(community_id, month(time))` over `(community_id, snapshot_id)`

**Decision:** Partition by `(community_id, month(time))`, not by `(community_id, snapshot_id)`.

`snapshot_id` is an application concept — one per billing period per community per export run. Using it as a partition key means every export creates new partition directories forever:

| Scenario | Partitions after 1 year | Partitions after 5 years |
|---|---|---|
| `(community_id, snapshot_id)`, 1 export/month | 300 × 12 = **3,600** | 18,000 |
| `(community_id, snapshot_id)`, 2 exports/month (corrections) | 300 × 24 = **7,200** | 36,000 |
| `(community_id, month(time))` — chosen | 300 × 12 = **3,600** | 18,000 |
| `(community_id, month(time))`, 2 exports/month | still **3,600** | still 18,000 |

With `snapshot_id` partitioning, re-exports due to late corrections double (or triple) the partition count. Iceberg metadata scanning degrades with partition count — above ~100k partitions a dedicated catalog service becomes necessary. At 300 communities the pressure is milder than at larger fleets, but the argument still holds: `month(time)` keeps the partition count flat under re-exports, while `snapshot_id` grows it linearly forever. With `month(time)`, re-exports append files to the same partition; Iceberg handles this efficiently and partition count stays bounded.

---

## 3. No hash bucketing on `community_id`

**Decision:** Use `IdentityTransform()` on `community_id`, not a `BucketTransform`.

Hash bucketing (`sha256(community_id)[:2]` or Iceberg's `BucketTransform(N)`) is designed to avoid S3 hot-partition throttling from alphabetical key prefixes. **AWS resolved this in 2018** — S3 now auto-scales per prefix regardless of naming. Hash bucketing on `community_id` would mix multiple communities' data into the same Parquet files, forcing full-file scans whenever a query filters by a single community. The access pattern is always per-community, so this is strictly worse.

---

## 4. `month(time)` over `identity(time)`

**Decision:** Use `MonthTransform()` on `time`, not `IdentityTransform()`.

`identity(time)` on a timestamp with 15-minute resolution would create one partition per 15-minute slot — ~2,976 partitions/community/month. `MonthTransform` groups an entire month into one partition, matching the billing period exactly.

---

## 5. Small Parquet files: accepted, cross-community compaction deferred

**Decision:** Accept small per-community Parquet files; do not pre-aggregate or pad. Add compaction only if cross-community queries become a performance requirement.

Parquet files at this partitioning level are small:

| Community size | Records/month | Compressed file size |
|---|---|---|
| 16 points (observed) | 238,080 | ~225 KB |
| 167 points (average) | ~2.48M | ~2.3 MB |
| 1,000 points | ~14.88M | ~14 MB |
| 4,000 points (outlier) | ~59.5M | ~56 MB |

The analytical sweet spot for Parquet is 128 MB – 1 GB per file. The average community partition is ~50× smaller than optimal; even the 4,000-point outlier is ~2× below. **This is not a problem for per-community queries** (the primary use case) — a `WHERE community_id = X AND snapshot_id = Y` query opens one file, which reads in milliseconds over S3 regardless of size.

It becomes a problem for **cross-community queries** (e.g. "sum all communities for January"): that opens 300 files, each requiring a separate S3 HTTP request (~10–50 ms each) — 3–15 seconds of pure I/O before reading any data. If this use case becomes a priority, the solution is periodic compaction via Iceberg's `rewrite_data_files()`, not a repartitioning.

> **Benchmark TODO:** the 3–15s estimate assumes sequential opens. DuckDB's `iceberg_scan` + `httpfs` fetches range reads in parallel, so the observed latency is likely 5–10× better. Before investing in compaction, measure an actual cross-community aggregation query against a realistic dataset (300 communities × 12 months) and record the number. Only compact if measured latency misses the downstream SLO.

---

## 6. Mandatory `snapshot_id` filter

**Decision:** Every query against `billing_exports.records` must include `WHERE snapshot_id = X`.

With `(community_id, month(time))` partitioning, multiple snapshot versions for the same period land in the **same partition directory**. A scan without a `snapshot_id` filter returns rows from all versions silently mixed together — wrong billing results, no error.

The read helpers in `reader.py` always require `snapshot_id` as a parameter. Document this explicitly when handing credentials and the `METADATA_URI` to downstream teams.

---

## 6a. Reader pins to sidecar URI; analytics resolves to latest

**Decision:** The billing read path uses the exact `iceberg_metadata_uri` recorded in the sidecar at write time. The analytics read path resolves to the latest metadata file in the table directory.

Two consumers, two requirements:

- **Billing must be reproducible.** An invoice rendered today and re-rendered next month must read the same bytes. Pinning to the sidecar's URI makes this true even after a v2 correction has been written into the same partition. The deterministic `snapshot_id` filter (§6) protects against version mixing; pinning the URI additionally protects against any future write that might shift what "latest" means.
- **Analytics wants freshness.** Cross-community queries are exploratory and want the current state of the warehouse, not the state at some historical sidecar's write time.

A single reader API that auto-resolves to "latest" satisfies analytics but quietly defeats billing reproducibility — the sidecar's `iceberg_metadata_uri` becomes decorative. The new `BillingReader.from_sidecar_uri()` (see the billing-pipeline interface spec) does **not** call the URI resolver; the existing `query_*` functions in `reader.py` keep their resolve-to-latest behaviour and are tagged as the analytics path.

---

## 7. Snapshot ID: deterministic hash over random

**Decision:** `make_snapshot_id(community_id, period_start, snapshot_version)` — a SHA-256 hash truncated to a signed int64 — rather than a random integer.

The prototype originally used `random.randrange(2**32)`. Problems:

- **Collision risk:** birthday paradox gives ~50% collision probability at 65,000 snapshots (~18 years at 300 communities/month, 1 export/month — corrections and retention extensions shorten this window).
- **No idempotency:** re-running the export creates a new snapshot for the same period, silently duplicating data.
- **Not debuggable:** a log entry with ID `3928349580` carries no human context.

Same community + same period + same version always produces the same ID. A crash-retry generates the same ID, which the idempotency check catches before a duplicate write occurs.

---

## 8. Write atomicity: single `table.append()` per community export

**Decision:** Collect all record batches in memory, concatenate into one PyArrow Table, and call `table.append()` once per community export.

Multiple `table.append()` calls per export would create intermediate Iceberg snapshots between calls. A concurrent reader querying mid-write would see an incomplete community export with no way to distinguish it from a complete one.

**Memory trade-off:** Peak memory is proportional to community size (not flat). For a typical community (~167 points) this is ~200 MB — acceptable for a batch worker. The **4,000-point outlier** is the pressure point: ~59.5M records × ~80 bytes ≈ **~4.8 GB** of Arrow data held in memory before the single `table.append()`. That is too large to keep on the commodity worker heap.

**Recommendation:** typical communities write in-memory. For communities above a threshold (e.g. > 500 metering points, tune from observation), stream record batches to a temporary Parquet file on local disk and pass the on-disk file to `table.append()`. This restores bounded memory while keeping the single-commit atomicity property: Iceberg still produces exactly one snapshot per community export regardless of whether the source batches came from RAM or disk.

The prototype in `prototype/src/writer.py` currently uses the in-memory path only. Before running against production outliers, add the disk-spill branch keyed on community size.

---

## 9. Idempotency: two-level guard

**Decision:** Idempotency is enforced at two levels — `write_community_snapshot()` prevents duplicate Iceberg commits; `write_snapshot()` additionally recovers from a crash between the Iceberg commit and the sidecar write.

**`write_community_snapshot()` — duplicate commit guard**

Before `table.append()`, a scan checks whether the `snapshot_id` is already committed. Because `snapshot_id` is deterministic, a retry produces the same ID and the check catches it. `SnapshotAlreadyExistsError` is the signal that the Iceberg commit has already landed.

**`write_snapshot()` — full idempotency including sidecar recovery**

`write_snapshot()` catches `SnapshotAlreadyExistsError` and branches on whether the sidecar exists:

| State | Trigger | Action |
|---|---|---|
| **First write** | `write_community_snapshot()` succeeds | Build metadata, write sidecar, return |
| **Clean retry** | `SnapshotAlreadyExistsError` + sidecar exists | Read and return existing sidecar |
| **Crash recovery** | `SnapshotAlreadyExistsError` + sidecar missing | Scan table for `record_count`, use `table.metadata_location`, write sidecar, return |

The recovered sidecar's `iceberg_metadata_uri` is `table.metadata_location` at recovery time, not at original commit time. This is correct: the sidecar is a discovery document — any metadata file that includes the target snapshot is valid for DuckDB reads.

**Orphan files:** A crash *during* `table.append()` leaves Parquet files without a committed Iceberg snapshot. These are cleaned up by Iceberg's `expire_snapshots()` + `remove_orphan_files()` maintenance operations. This case is distinct from crash recovery: no rows are committed, so a retry writes fresh data normally.

---

## 10. Sidecar: discovery document, not audit seal

**Decision:** The sidecar carries business context and a pointer to the Iceberg metadata file. It does not carry cryptographic hashes of the Parquet files.

The Iceberg Parquet files contain only integers and timestamps. The sidecar carries the business context needed to interpret them: community names, metering point IDs, energy direction, registration validity periods. Without it, the Parquet files are opaque to a reader who only has S3 access.

**Why no Parquet hashes:** Parquet files may be rewritten by compaction (`rewrite_data_files()`). If the sidecar contained file-level hashes, compaction would invalidate them. The `snapshot_id` is the durable reference — it identifies the same logical rows regardless of physical file layout (compaction, rewrites, reorganisation).

**Cryptographic sealing is the billing pipeline's responsibility.** When the billing pipeline generates an invoice, it seals the evidence: the invoice, the `snapshot_id` it queried, and a hash of the data it consumed. This separation allows the archival pipeline to remain simple and compaction to run freely without touching any audit trail.

---

## 11. Data extraction: `.values_list()` + chunked Arrow over ORM iteration

**Decision:** Use `.values_list()` with chunked PyArrow batches rather than iterating Django ORM instances.

The ORM approach iterates record-by-record and instantiates a Python object per row. At scale: 50,000 metering points × ~14,880 records = **744M rows/month**. At 1 µs per object (optimistic), that is 744 seconds per full export run, plus significant memory pressure.

`.values_list()` with chunked Arrow batches is a single-line change with 10–50× throughput improvement.

**Better option (future):** Use `adbc-driver-postgresql` or `psycopg3` binary cursor to stream Arrow record batches directly from PostgreSQL, bypassing the Django ORM entirely. This is the path for full-scale 744M row exports.

---

## 12. Schema: corrections from the initial design

The following fields differ from the first version of the schema. All corrections are reflected in the current `spec.md` and `src/schema.py`.

| Field | Initial design | Current | Why |
|---|---|---|---|
| `time` | `TimestampType()` (no tz) | `TimestamptzType()` (UTC) | DST transitions affect the SNAP window (10–16h local time). 2 days/year produce wrong SNAP/NON-SNAP splits with naive timestamps. |
| `message_created_at` | `TimestampType()` | `TimestamptzType()` | Consistency; avoids silent offset loss. |
| `value` | `required=True` | `required=False` | Missing meter readings are a real domain event. Forcing non-null means the pipeline either crashes or silently substitutes a value. |
| `exported_at` | absent | `TimestamptzType()` required | Records when this row entered the archive. Essential for audit: "was this record available at the time the invoice was issued?" |
| `community_id` | `IntegerType()` + `uint8` in PyArrow | `IntegerType()` + `pa.int32()` | Type mismatch between Iceberg and PyArrow causes silent data corruption. Iceberg schema is the authority; PyArrow must match exactly. |
| `snapshot_id` | `LongType()` + `uint64` in PyArrow | `LongType()` + `pa.int64()` | Same mismatch. High-bit values coerce incorrectly between signed and unsigned 64-bit. |
| `snapshot_version` | absent | `IntegerType()` required (field 11) | Parquet files must be self-describing. A reader with only data files (no sidecar, no registry) needs to identify the version. |

---

## 13. Version accumulation: read degradation profile

**Decision:** Accept linear read degradation with version count; mitigate via compaction if it becomes a concern.

Because `snapshot_id` is not a partition field, multiple snapshot versions for the same community/period land in the same partition directory as separate Parquet files. A query with `WHERE community_id = X AND snapshot_id = Y` must open and filter *all* files in that partition, discarding rows from other versions.

Read latency for a single community therefore grows linearly with the number of re-export versions. For 1–3 versions this is negligible. The benchmark (`bench_read.py` version accumulation test) establishes the degradation curve and confirms it remains sub-second up to at least 5 versions.

**Mitigation if needed:** Periodic `rewrite_data_files()` compaction that physically separates versions into distinct files with row-group-level min/max statistics on `snapshot_id`, enabling file-level pruning. Not needed at current correction rates.
