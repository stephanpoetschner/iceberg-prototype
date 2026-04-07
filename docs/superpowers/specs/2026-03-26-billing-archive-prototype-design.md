# Billing Archive Prototype — Technical Spec

**Status**: Draft
**Date**: 2026-03-26
**Parent**: [Project Overview](../../project-overview.md), [SPEC.md](../../../SPEC.md)

---

## 1. Purpose

Validate the Iceberg/Parquet/DuckDB architecture described in SPEC.md with a standalone prototype before production integration. The prototype demonstrates the full write-and-read cycle using synthetic data at realistic scale, with performance benchmarks covering throughput, latency, file sizes, and memory consumption.

This prototype is the prerequisite for:
- Redesigning the billing pipeline to support dynamic tariffs (P1)
- Establishing the archive as the system of record for settled periods (P2)
- Enabling analytical and third-party data access (P3)

---

## 2. Scope

### In scope

- Synthetic data generation matching production characteristics
- Iceberg table creation with the schema and partitioning from SPEC.md
- Write path: synthetic records → PyArrow → PyIceberg → Parquet on local filesystem or MinIO
- Read path: DuckDB queries against Iceberg without catalog access
- Metadata sidecar generation (business context and discovery)
- Performance benchmarks: write throughput, read latency, file sizes, memory consumption
- Memory profiling to verify bounded memory usage (Memray for Python allocations, OS-level RSS for DuckDB)
- Atomic write validation: each community export commits as a single Iceberg transaction
- Idempotency validation: duplicate writes are detected and rejected
- Concurrent write validation: multiple processes writing different communities simultaneously
- DST boundary correctness for SNAP/NON-SNAP billing queries

### Out of scope

- Django/Celery integration
- Real data from TimescaleDB
- Production S3 storage (local filesystem or MinIO only)
- Completeness gate logic (MonthlyReport status checks)
- Snapshot registry Django model (BillingSnapshot)
- Billing pipeline consumer implementation

---

## 3. Standalone Project Structure

The prototype is a standalone Python project, not embedded in the Django application. It depends only on PyIceberg, PyArrow, DuckDB, Pydantic, and testing/profiling libraries.

```
iceberg-records/
  prototype/
    src/
      schema.py          # Iceberg + PyArrow schema definitions (from SPEC.md §5)
      synthetic.py        # Deterministic synthetic data generator
      writer.py           # Write path: Arrow batches → Iceberg, chunked
      reader.py           # Read path: DuckDB query helpers
      sidecar.py          # Metadata sidecar generation (business context and discovery)
      snapshot_id.py      # Deterministic snapshot ID computation
    tests/
      conftest.py         # Shared fixtures: iceberg_table, warehouse_dir, sidecar_dir
      test_schema.py      # Schema round-trip and type-match validation
      test_write_read.py  # Correctness: write then read, verify data integrity
      test_sidecar.py     # Sidecar generation and metadata validation
      test_snapshot_id.py # Deterministic ID, collision resistance
    benchmarks/
      bench_write.py      # Write throughput at varying scale
      bench_read.py       # Read latency for billing query patterns
      bench_memory.py     # Memray-based memory profiling
    pyproject.toml
```

---

## 4. Synthetic Data Design

Generate data matching production characteristics. The generator must be **deterministic** (seeded) so benchmarks are reproducible.

| Parameter | Value | Source |
|---|---|---|
| Communities | Configurable, default 100 (scale to 2,500 for perf tests) | SPEC.md §3 |
| Metering points per community | 5–200, configurable distribution (avg ~20) | SPEC.md §3 |
| Resolution | 15-min slots = 96/day = 2,976/month | Given |
| Record types per slot | ~5 | SPEC.md §3: observed ratio |
| Records per community per month (avg) | ~297,600 | 20 points × 14,880 |
| Value range | Decimal, 0.000000–50.000000 kWh | Realistic range |
| Null rate | ~2% of values are None | Missing readings are a real domain event |
| Timestamps | UTC, microsecond precision | SPEC.md §5.2 |

### Outlier communities

To validate memory behavior, the generator must support outlier communities:

| Profile | Metering points | Records/month | Purpose |
|---|---|---|---|
| Small | 5 | ~74,400 | Lower bound |
| Average | 20 | ~297,600 | Typical case |
| Large | 50 | ~744,000 | Common large community |
| Outlier | 200 | ~2,976,000 | Stress test for memory profiling |

---

## 5. Schema

Based on SPEC.md §5.1 and §5.2, with one correction: `snapshot_version` is added as a column in the Iceberg table. SPEC.md §7.1 states "use a separate `snapshot_version` column" but omits it from the schema definitions in §5.1/§5.2. The prototype includes it so that Parquet files are self-describing — a reader with only the data files (no sidecar, no registry) can determine which version a row belongs to.

### Iceberg schema (§5.1 + snapshot_version)

11 fields: `snapshot_id` (LongType), `community_id` (IntegerType), `time` (TimestamptzType), `record_type` (IntegerType), `ec_registration_id` (LongType), `metering_point_id` (LongType), `value` (DecimalType(22,6) nullable), `value_type` (IntegerType), `message_created_at` (TimestamptzType), `exported_at` (TimestamptzType), `snapshot_version` (IntegerType, default 1).

### PyArrow schema (§5.2 + snapshot_version)

Must mirror Iceberg exactly. Key mappings:
- `TimestamptzType()` → `pyarrow.timestamp("us", tz="UTC")`
- `DecimalType(22, 6)` → `pyarrow.decimal128(22, 6)`
- `LongType()` → `pyarrow.int64()` (not uint64)
- `IntegerType()` → `pyarrow.int32()` (not uint8)
- `snapshot_version` → `pyarrow.int32()` (not nullable)

### Schema validation test

The prototype must include a test that creates an Iceberg table, writes a batch via PyArrow, reads it back, and verifies every field type matches. This catches the exact type mismatch bugs identified in SPEC.md §5.1.

---

## 6. Partitioning

As specified in SPEC.md §6.1:

```
PartitionSpec(
    PartitionField(source_id=2, field_id=100, transform=IdentityTransform(), name="community_id"),
    PartitionField(source_id=3, field_id=101, transform=MonthTransform(),    name="time_month"),
)
```

The prototype must verify that:
- Re-exports (corrections) land in the same partition directory
- Queries with `community_id` and `snapshot_id` filters only read the relevant partition
- Partition count matches expectations (communities × months)

---

## 7. Write Path

### 7.1 Atomic single-commit writes

Each community/period export must be a **single Iceberg commit**. Multiple `table.append()` calls per export create intermediate Iceberg snapshots that expose partial data to concurrent readers — a reader querying mid-write would see an incomplete community export with no way to distinguish it from a complete one.

**Strategy**: Generate data in chunks (for memory during generation), collect the RecordBatches, concatenate into one PyArrow Table, and call `table.append()` once.

```
for each community/period:
    compute snapshot_id (deterministic)
    check idempotency: reject if snapshot_id already exists (§7.4)
    batches = []
    for each chunk of records (CHUNK_SIZE = 100,000):
        build PyArrow RecordBatch from chunk
        batches.append(batch)
    full_table = pyarrow.concat_tables([b.to_table() for b in batches])  # zero-copy chunked
    table.append(full_table)   # single Iceberg commit — atomic
    capture metadata_location
    write metadata sidecar  # no Parquet hashes — cryptographic sealing is billing pipeline's responsibility
```

### 7.1.1 `write_snapshot()` orchestrator

The write path is exposed via a single `write_snapshot()` function in `writer.py` that wires the Iceberg commit to the sidecar write. `write_community_snapshot()` remains unchanged — all existing tests call it directly.

`write_snapshot()` is **fully idempotent**: a retry with the same arguments always succeeds. It handles three states (see §7.4 for the crash recovery case):

| State | Trigger | Action |
|---|---|---|
| **First write** | `write_community_snapshot()` succeeds | Build metadata, write sidecar, return |
| **Clean retry** | `SnapshotAlreadyExistsError` + sidecar exists | Read and return existing sidecar |
| **Crash recovery** | `SnapshotAlreadyExistsError` + sidecar missing | Query table for `record_count`, use `table.metadata_location`, write sidecar, return |

The caller receives `BillingSnapshotMetadata` in all three cases; `iceberg_metadata_uri` is a field on it.

**Memory tradeoff**: Peak memory is proportional to community size, not flat. The worst case is the 200-point outlier (~3M records × ~80 bytes ≈ 240MB), which is acceptable for a batch process. Atomicity is more important than flat memory here — a partial write is a data integrity bug; 240MB of memory is not.

**Future option**: If communities grow significantly beyond 200 points, write chunks to a temporary Parquet file on local disk, then use `table.append()` from the on-disk file. This restores flat memory while keeping atomicity. Not needed at current scale.

### 7.2 Snapshot ID

Deterministic, based on SPEC.md §7.1 with a correction: `snapshot_version` is included in the hash input. SPEC.md §7.1 shows a two-parameter function but states in the text to "include it in the ID hash." The prototype uses the corrected three-parameter version so that corrections produce distinct IDs:

```python
def make_snapshot_id(community_id: int, period_start: datetime, snapshot_version: int = 1) -> int:
    key = f"{community_id}:{period_start.isoformat()}:{snapshot_version}"
    return int(hashlib.sha256(key.encode()).hexdigest()[:16], 16) & 0x7FFFFFFFFFFFFFFF
```

### 7.3 Catalog

PostgreSQL for the prototype, matching the production environment. The prototype uses a local PostgreSQL instance (e.g., via Docker Compose) as the Iceberg SQL catalog. This validates concurrent-write behavior early rather than discovering catalog-level issues only in production.

SQLite is explicitly rejected: it cannot handle concurrent commits from multiple processes, and switching catalogs between prototype and production risks masking real issues (locking semantics, transaction isolation, commit conflict behavior all differ).

### 7.4 Idempotency

Idempotency is handled at two levels: `write_community_snapshot()` prevents duplicate Iceberg commits; `write_snapshot()` additionally recovers from a crash between the Iceberg commit and the sidecar write.

**`write_community_snapshot()` — duplicate commit guard**

Before `table.append()`, a scan checks whether the `snapshot_id` is already committed:

```python
existing = table.scan(
    row_filter=EqualTo("snapshot_id", snapshot_id)
).to_arrow()
if len(existing) > 0:
    raise SnapshotAlreadyExistsError(snapshot_id)
```

Because `snapshot_id` is deterministic (same community + period + version = same ID), a retry produces the same ID and the check catches it. `SnapshotAlreadyExistsError` is the signal that the Iceberg commit has already landed.

**`write_snapshot()` — full idempotency including sidecar recovery**

`write_snapshot()` catches `SnapshotAlreadyExistsError` and branches on whether the sidecar exists:

- **Sidecar present** → clean retry: read and return the existing sidecar.
- **Sidecar missing** → crash recovery: the Iceberg commit landed but the process died before `write_sidecar()`. The sidecar is reconstructed from the caller's arguments plus a table scan for `record_count` and `table.metadata_location` for `iceberg_metadata_uri`.

The recovered sidecar's `iceberg_metadata_uri` is `table.metadata_location` at recovery time, not at original commit time. This is correct: the sidecar is a discovery document, not an audit seal (§9). Any metadata file that includes the target snapshot is valid for DuckDB reads, and the current location is always valid.

**Orphan files**

If the crash happens *during* `table.append()` (after Parquet files are written but before Iceberg metadata is committed), the Parquet files become orphans. These are cleaned up by Iceberg's `expire_snapshots()` + `remove_orphan_files()` maintenance operations (see SPEC.md §13 item 4). This case is distinct from the sidecar recovery case: no rows are committed, so a retry writes fresh data normally.

### 7.5 Concurrent writes

The prototype must validate that two processes can write different communities simultaneously without catalog corruption. This is tested with `multiprocessing` — two workers each exporting a distinct community/period. The test asserts:
- Both commits succeed
- No data interleaving (each community's records are complete and independent)
- Catalog metadata is consistent after both writes

### 7.6 Version discovery — determining the next snapshot_version before writing

`snapshot_version` is not tracked internally by the write path. The caller is responsible for supplying the correct version number. For version=1 (original export) this is trivial, but for corrections the caller must determine what the highest committed version is before writing the next one.

The authoritative source is the Iceberg table itself: `snapshot_version` is stored in every row (§5), so it reflects exactly what is committed, regardless of whether sidecars are present or whether a previous process crashed mid-write.

**Query — next version for a community/period:**

```sql
SELECT MAX(snapshot_version) AS max_version
FROM iceberg_scan('{metadata_uri}')
WHERE community_id = {community_id}
  AND time >= '{period_start}'
  AND time <  '{period_end}'
```

- If the result is `NULL` (no rows): use `snapshot_version = 1` (first export).
- Otherwise: use `max_version + 1`.

This query reads all Parquet files in the `community_id` partition (version accumulation applies — see §8.6), but returns quickly because only one aggregated value is needed per file. At 1–5 versions it is negligible.

**Alternative — read the sidecar directory**: list `<sidecar_dir>/` for `*.sidecar.json` files, parse each, filter by `community_id` + `period_start`, take `max(snapshot_version) + 1`. This is simpler but unreliable: a sidecar is written *after* the Iceberg commit, so a crash between the two leaves committed rows with no sidecar. The Iceberg table is the ground truth.

---

## 8. Read Path

Demonstrate the DuckDB query patterns that downstream consumers will use.

### 8.1 Single-community billing query

The primary use case. Retrieve all records for a specific community, period, and snapshot:

```sql
SELECT * FROM iceberg_scan('{metadata_uri}')
WHERE community_id = {community_id}
  AND snapshot_id = {snapshot_id}
```

### 8.2 SNAP / NON-SNAP aggregation

The billing query for dynamic tariffs. SNAP hours are 10:00–16:00 local Austrian time:

```sql
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
FROM iceberg_scan('{metadata_uri}')
WHERE community_id = {community_id}
  AND snapshot_id = {snapshot_id}
GROUP BY 1, 2
```

### 8.3 Per-registration detail

Full quarter-hour detail for a single registration — the data needed for per-slot dynamic pricing:

```sql
SELECT time, record_type, value, value_type
FROM iceberg_scan('{metadata_uri}')
WHERE community_id = {community_id}
  AND snapshot_id = {snapshot_id}
  AND ec_registration_id = {registration_id}
ORDER BY time
```

### 8.4 Cross-community aggregation

Preview of P3 analytics use case. Sum across all communities for one month:

```sql
SELECT
    community_id,
    record_type,
    SUM(value) AS total
FROM iceberg_scan('{metadata_uri}')
WHERE snapshot_id IN ({snapshot_ids})
GROUP BY 1, 2
```

This query is expected to be slow due to small file sizes (SPEC.md §6.5). The benchmark establishes the baseline and documents the I/O overhead. In practice, adding a `time` range filter (e.g. `WHERE time >= '2026-01-01' AND time < '2026-02-01'`) enables month-partition pruning and should be included in production queries.

### 8.5 Read-path memory bounding

DuckDB materializes query results in memory by default. For single-community queries (~300k records) this is fine. For cross-community queries at full scale (~744M records), unbounded result materialization would exhaust memory.

Read benchmarks must use DuckDB's streaming result API (`fetchmany()` iteration) or `LIMIT`/pagination to bound memory. The memory benchmarks in section 10.3 verify this.

### 8.6 Version accumulation and read degradation

Because `snapshot_id` is not a partition field, multiple snapshot versions for the same community/period land in the same partition directory as separate Parquet files. A query with `WHERE community_id = X AND snapshot_id = Y` must open and filter *all* files in that partition, discarding rows from other versions.

This means read latency for a single community grows linearly with the number of re-export versions in that partition. For 1–3 versions this is negligible. The prototype must benchmark this explicitly (see §10.2) to establish the degradation curve and confirm it remains sub-second up to at least 5 versions.

If degradation becomes a concern in production, the mitigation is periodic Iceberg `rewrite_data_files()` compaction that physically separates versions into distinct files with row-group-level min/max stats on `snapshot_id`, enabling file-level pruning.

### 8.7 Mandatory snapshot_id filter — safety net

A query without a `snapshot_id` filter silently returns rows from *all* versions mixed together — wrong billing data with no error. The prototype's `reader.py` query helpers must always require `snapshot_id` as a parameter. There is no valid use case for reading without it.

The prototype includes a **test that deliberately omits the `snapshot_id` filter** after writing two versions, and asserts the row count is doubled — documenting the failure mode explicitly so downstream teams understand the risk.

### 8.8 Version discovery — resolving the latest snapshot_id for a community/period

Before the billing pipeline can issue a §8.1–8.3 query it needs a `snapshot_id`. If corrections have been written, there will be multiple versions in the partition; the pipeline must resolve the latest one.

**Query — latest snapshot_id for a community/period:**

```sql
SELECT snapshot_id, snapshot_version
FROM iceberg_scan('{metadata_uri}')
WHERE community_id = {community_id}
  AND time >= '{period_start}'
  AND time <  '{period_end}'
GROUP BY snapshot_id, snapshot_version
ORDER BY snapshot_version DESC
LIMIT 1
```

The result row's `snapshot_id` is then passed directly to §8.1–8.3 queries. The result's `snapshot_version` can be logged or stored alongside the invoice for auditability.

**Why query the table rather than the sidecar directory**: the sidecar is written after the Iceberg commit (§7.1 sequence step 4). A process that crashed between the commit and the sidecar write leaves committed rows with no corresponding sidecar. Querying the table is always correct; the sidecar is a convenience cache, not the source of truth.

**When to run this**: once per community/period at the start of an invoice run, before any billing queries. Cache the resolved `snapshot_id` for the duration of the invoice — do not re-resolve mid-run, as a concurrent correction could change it.

---

## 9. Metadata Sidecar

The sidecar is a **metadata and discovery document**, not an audit seal. It carries the business context needed to interpret the Parquet files (community names, metering point IDs, registration periods) and the `iceberg_metadata_uri` needed to find them. It does not carry cryptographic hashes of the Parquet files.

**Cryptographic sealing is the billing pipeline's responsibility** (out of scope for prototype). When the billing pipeline generates an invoice, it seals the evidence: the invoice, the `snapshot_id` it queried, and a hash of the data it consumed. The `snapshot_id` is the stable reference — it identifies the same logical rows regardless of whether the underlying Parquet files have been compacted, rewritten, or reorganized.

This separation means the archival pipeline is simple (write correct, atomic, immutable data) and compaction can run freely on `billing_exports.records` for analytics performance without breaking any audit trail.

The prototype uses the `BillingSnapshotMetadata` Pydantic schema with synthetic values for Django-sourced fields (`community_name`, `community_ec_id`, `ec_registrations`, `metering_points`). This validates the serialization pipeline even though the values are not from a real database.

### 9.1 Sidecar contents

The sidecar includes:
- `snapshot_id`, `snapshot_version`, `community_id` — identity
- `community_name`, `community_ec_id` — business context
- `period_start`, `period_end` — billing period
- `record_count` — quick validation (compare to `SELECT COUNT(*)`)
- `exported_at` — when the archive was written
- `iceberg_metadata_uri` — pointer to the Iceberg metadata file at time of write
- `ec_registrations`, `metering_points` — the registrations and points active during the period

The sidecar does **not** include `parquet_sha256` or any file-level hashes. Parquet files may be rewritten by compaction; the `snapshot_id` is the durable reference to the logical data.

The prototype must verify:
- Sidecar JSON is valid and round-trips through the Pydantic model
- `iceberg_metadata_uri` in the sidecar points to a valid metadata file
- `record_count` matches actual `SELECT COUNT(*) WHERE snapshot_id = X`

### 9.2 Sidecar path convention

- **Directory**: caller-supplied `sidecar_dir`, expected to be `<warehouse_dir>/sidecars/`
- **Filename**: `<snapshot_id>.sidecar.json` — unique, no collisions, easily listed

The path is not derived from `iceberg_metadata_uri` to avoid coupling path logic to the URI format (which already has its own resolution quirks in `_resolve_latest_metadata`).

### 9.3 `exported_at` semantics

`exported_at` is set **once per community export**, not per chunk. All records in a single export carry the same `exported_at` value. This means:
- A reader can identify all records from a specific export run by `exported_at`
- Auditing "was this record available at invoice time?" uses this single timestamp
- There is no ambiguity from per-chunk timestamps drifting during a long write

The `exported_at` value is computed at the start of the export and passed to the batch builder, not generated per-row.

---

## 10. Performance Test Plan

### 10.1 Write benchmarks

| Test | Parameters | What it measures |
|---|---|---|
| Single community write | 1 community, avg 20 points, 1 month | Baseline write latency |
| Batch write | 100 communities, avg 20 points, 1 month | Sequential throughput (records/sec) |
| Scale write | 2,500 communities, avg 20 points, 1 month | Full production-scale export time |
| Outlier write | 1 community, 200 points, 1 month (~3M records) | Large community handling |

### 10.2 Read benchmarks

| Test | Parameters | What it measures |
|---|---|---|
| Single community read | `WHERE community_id = X AND snapshot_id = Y` | Query latency, target sub-second |
| SNAP/NON-SNAP aggregation | Billing query on single community | Aggregation latency |
| Per-registration detail | Full QH data for one registration | Detail query latency |
| Cross-community read | All 2,500 communities, one month | Small-file I/O overhead |
| Version accumulation degradation | Same community with 1, 2, 3, 5 re-export versions, measure single-community read latency for each | Read latency growth per version |
| DST transition SNAP/NON-SNAP | Billing query on March and October data spanning DST switch | Correctness under DST boundary |

### 10.3 Memory benchmarks

**Goal**: Prove that peak memory stays bounded and proportional to community size (due to atomic writes), and that the maximum (200-point outlier ≈ 240MB) is acceptable.

**Write path** — tracked via Memray (Python allocations):

| Test | Parameters | What it measures |
|---|---|---|
| Write memory scaling | Write communities of 5, 20, 50, 200 points under Memray | Peak RSS scales linearly with community size, max ~240MB |
| Chunk size sensitivity | Same community, varying CHUNK_SIZE (10k, 50k, 100k, 500k) | Memory during generation phase vs. chunk size |

**Read path** — tracked via OS-level RSS (not Memray):

DuckDB is a C++ library. Its memory allocations do not appear in Memray's Python-level tracking. Read-path memory must be measured via OS-level peak RSS (`resource.getrusage(resource.RUSAGE_SELF).ru_maxrss`) or `/proc/self/status` VmRSS tracking, plus DuckDB's own `PRAGMA database_size` and `SET memory_limit` configuration.

| Test | Parameters | What it measures |
|---|---|---|
| Read memory scaling | Read communities of 5, 20, 50, 200 points, measure OS-level peak RSS | RSS stays bounded for single-community queries |
| Cross-community read memory | Streaming `fetchmany()` over all 2,500 communities | RSS stays bounded during streaming iteration |

**Red flag**: If peak RSS for a 200-point write exceeds ~300MB (allowing ~60MB overhead beyond the ~240MB data), investigate — the pipeline is copying data unnecessarily.

### 10.4 Data integrity tests

| Test | What it validates |
|---|---|
| Schema round-trip | Write via PyArrow, read via DuckDB, verify all field types and values match |
| Row count round-trip | For every write, immediately `SELECT COUNT(*)` via DuckDB and assert exact match against expected record count. This is a mandatory post-condition on all write benchmarks, not a separate optional test |
| Re-export coexistence | Write version=1 and version=2 for same community/period, query each independently, verify each returns only its own rows |
| Missing snapshot_id filter | Write version=1 and version=2, query without `snapshot_id` filter, assert row count equals sum of both versions — documents the failure mode explicitly |
| Snapshot ID determinism | Same inputs produce same ID; different inputs produce different IDs |
| Snapshot ID collision resistance | Generate IDs for 2,500 communities × 12 months × 5 versions × 10 years = 1.5M IDs, verify zero collisions. The 90k-ID test in the original spec is too small to be meaningful at the 63-bit hash space |
| Null value handling | Write records with null `value`, verify they survive round-trip as null (not 0, not dropped) |
| Null aggregation correctness | Write a known pattern of null and non-null values, run `SUM()`, `COUNT(*)`, and `COUNT(value)` via DuckDB, verify: SUM skips nulls, COUNT(*) includes null rows, COUNT(value) excludes them |
| Sidecar record_count | Write a community, generate sidecar, verify `record_count` matches `SELECT COUNT(*) WHERE snapshot_id = X` |
| Sidecar integration round-trip | `test_write_snapshot_sidecar_integration`: call `write_snapshot()` end-to-end; assert (1) `metadata.iceberg_metadata_uri` points to a file that exists on disk, (2) `metadata.record_count` equals `SELECT COUNT(*) WHERE snapshot_id = X` via DuckDB, (3) `read_sidecar(<sidecar_dir>/<snapshot_id>.sidecar.json)` round-trips to the same `BillingSnapshotMetadata`. Uses synthetic values for Django-sourced fields (`community_name`, `community_ec_id`, `ec_registrations`, `metering_points`). This satisfies success criterion 7. |
| Idempotency — duplicate write rejection | Write community/period/version=1, attempt identical write again, assert `SnapshotAlreadyExistsError` is raised and row count does not change |
| Atomic write visibility | Start a long write (200-point outlier) in a subprocess. In the main process, poll with `SELECT COUNT(*)` queries during the write. Assert: count is either 0 (not yet committed) or the full expected count (committed). Never a partial count |
| Concurrent writes | Two `multiprocessing` workers write different communities simultaneously to the same Iceberg table via PostgreSQL catalog. Assert: both succeed, no interleaving, catalog metadata is consistent |
| DST transition — March (spring forward) | Generate data for 2026-03-29 (CET→CEST switch at 02:00). Run SNAP/NON-SNAP query. Verify that the hour 02:00–03:00 CET (which is skipped) produces no misclassification. The 15-min slot at 01:45 CET is NON-SNAP; the next slot at 03:00 CEST is NON-SNAP. No slot should be classified differently than when computed manually |
| DST transition — October (fall back) | Generate data for 2026-10-25 (CEST→CET switch at 03:00). The hour 02:00–03:00 occurs twice. Run SNAP/NON-SNAP query and verify totals match a manually computed reference. This is the harder case — two physical hours map to the same wall-clock hour |
| Decimal precision | Write values with 6 decimal places (e.g., `1.123456`), read back via DuckDB, assert exact equality with no precision loss |
| `exported_at` consistency | Write a community export with known `exported_at` timestamp. Read back all rows, assert every row has the identical `exported_at` value (not per-chunk variation) |

### 10.5 What to look for (red flags)

- **Memory growth**: Peak RSS during writes exceeding ~300MB for any community size
- **Write degradation**: Throughput dropping as partition count increases
- **Query planning overhead**: DuckDB query planning time growing with Iceberg metadata size
- **Cross-community I/O wall**: Many small files causing excessive filesystem/S3 latency
- **Type mismatches**: PyArrow ↔ Iceberg type disagreements (the bug SPEC.md §5.1 explicitly warns about)
- **Snapshot ID collisions**: Any collision in the generated ID space
- **Decimal precision loss**: Values losing precision through the write/read cycle
- **Partial write visibility**: Any test observing a partial row count during an in-progress write
- **Version accumulation slowdown**: Single-community read latency exceeding 1 second after 5 re-export versions
- **DST misclassification**: Any SNAP/NON-SNAP aggregation error during DST transition dates
- **Catalog contention**: Concurrent writes failing with lock timeouts or serialization errors on PostgreSQL

---

## 11. Success Criteria

The prototype succeeds if:

1. **Correctness**: All write/read paths produce correct results. Data round-trips without loss, type coercion, or precision degradation. Row counts match exactly on every write/read cycle.
2. **Bounded memory**: Write-path peak memory scales linearly with community size, capped at ~300MB for the 200-point outlier. Read-path peak RSS stays bounded when using streaming iteration.
3. **Read latency**: Single-community billing queries complete in sub-second time, including up to 5 re-export versions in the same partition.
4. **File sizes**: Parquet files match SPEC predictions (~225KB for 16-point community, ~280KB for 20-point average, scaling linearly).
5. **Partition count**: Matches expectations (communities × months), does not grow with re-exports.
6. **Re-export safety**: Correction snapshots (version=2+) coexist with originals without data corruption. A missing `snapshot_id` filter demonstrably returns duplicates (documenting the risk).
7. **Sidecar integrity**: Sidecar metadata is valid, `record_count` matches actual data, `iceberg_metadata_uri` points to a valid metadata file. Cryptographic sealing is deferred to the billing pipeline (out of scope).
8. **Schema fidelity**: Zero type mismatches between PyIceberg and PyArrow schemas.
9. **Atomicity**: No partial writes are visible to concurrent readers during export. A community export is either fully visible or not visible at all.
10. **Idempotency**: Duplicate writes (same community + period + version) are rejected. Row count does not change on retry.
11. **Concurrent writes**: Two processes writing different communities simultaneously via PostgreSQL catalog both succeed without data corruption or catalog errors.
12. **DST correctness**: SNAP/NON-SNAP billing aggregations produce correct results across both DST transitions (March spring-forward, October fall-back).

---

## 12. Technology Stack

| Component | Choice | Notes |
|---|---|---|
| Language | Python 3.12+ | Match production environment |
| Table format | Apache Iceberg | Via PyIceberg |
| File format | Parquet | Via PyArrow |
| Query engine | DuckDB | With iceberg extension; pin versions in pyproject.toml |
| Catalog | PostgreSQL | Matches production; validates concurrent-write behavior. Local instance via Docker Compose |
| Storage (prototype) | Local filesystem or MinIO | Production uses S3 |
| Schema validation | Pydantic | For metadata sidecar |
| Memory profiling (Python) | Memray | For write-path memory benchmarks |
| Memory profiling (DuckDB) | OS-level RSS tracking | `resource.getrusage` / `/proc/self/status`; Memray cannot see C++ allocations |
| Testing | pytest | With pytest-benchmark for perf tests |
| Data generation | Deterministic (seeded) | For reproducible benchmarks |

---

## 13. Deviations from SPEC.md

These are intentional corrections to issues found in SPEC.md during review. SPEC.md should be updated to match.

| Item | SPEC.md | This spec | Why |
|---|---|---|---|
| `snapshot_version` column | Mentioned in §7.1 text but absent from §5.1/§5.2 schema definitions | Added as field 11 in both schemas | Parquet files should be self-describing; readers without sidecar access need to identify the version |
| `make_snapshot_id` signature | Two parameters: `(community_id, period_start)` | Three parameters: `(community_id, period_start, snapshot_version)` | SPEC.md §7.1 text says to include version in the hash, but the code sample omits it. Without it, version=1 and version=2 of the same community/period collide |
| Prototype catalog | SPEC.md §4.2 recommends PostgreSQL for production only; prototype could use SQLite | PostgreSQL for the prototype too | Catalog behavior under concurrency is a critical risk. SQLite masks locking/isolation issues that will surface in production with Celery workers. Test with the real catalog from day one |
| Write atomicity | SPEC.md §9.2 implies chunked appends (multiple `table.append()` per community) | Single `table.append()` per community export | Multiple appends create intermediate Iceberg snapshots visible to concurrent readers, exposing partial data. Atomicity is more important than flat memory at this scale |
| Memory model | Flat memory regardless of community size | Bounded memory proportional to community size (max ~240MB) | Consequence of atomic writes. 240MB for a 200-point outlier is acceptable for a batch process. If communities grow much larger, a disk-based staging approach can restore flat memory |
| Audit trail location | Parquet SHA-256 hashes in archival sidecar | No hashes in archival pipeline; cryptographic sealing deferred to billing pipeline | The billing pipeline is the consumer that needs to prove "this invoice was based on this data." It seals at consumption time using `snapshot_id` as the stable reference. This decouples the archival pipeline from audit concerns and allows compaction to run freely |
