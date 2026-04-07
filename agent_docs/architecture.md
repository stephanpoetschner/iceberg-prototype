# Architecture Reference

## Prototype Module Layout (`prototype/src/`)

| File | Responsibility |
|---|---|
| `schema.py` | `ICEBERG_SCHEMA`, `ARROW_SCHEMA`, `PARTITION_SPEC` — single source of truth for all type definitions |
| `snapshot_id.py` | `make_snapshot_id(community_id, period_start, snapshot_version)` — deterministic SHA-256-derived ID; `SnapshotAlreadyExistsError` |
| `synthetic.py` | Seeded record generator — produces realistic 15-min metering records without a database |
| `writer.py` | `write_snapshot()` (full pipeline) and `write_community_snapshot()` (Iceberg-only); chunked Arrow batch construction |
| `sidecar.py` | `BillingSnapshotMetadata` Pydantic model; `write_sidecar()` / `read_sidecar()` |
| `reader.py` | DuckDB query helpers — all require `snapshot_id` filter |
| `lock.py` | Redis-backed `snapshot_lock()` context manager for concurrent-write safety |

## Storage Layout

```
warehouse/                          ← local filesystem (prototype) or S3 (production)
  billing_exports/
    records/                        ← Iceberg table (PyIceberg-managed, do not write manually)
      metadata/
      data/
        community_id=127/
          time_month=2026-01/
            {uuid}.parquet
snapshots/                          ← Sidecar files (pipeline-managed)
  {community_id}/
    {period}/
      {snapshot_id}.sidecar.json
```

## Docker Services

| Service | Port | Purpose |
|---|---|---|
| PostgreSQL 16 | 5432 | Iceberg SQL catalog (`iceberg/iceberg/iceberg_test`) |
| Redis 7 | 6379 | Distributed lock for concurrent snapshot writes |
| MinIO | 9000/9001 | S3-compatible storage for S3 integration tests |

## Test Infrastructure

`conftest.py` provides session-scoped fixtures: `catalog` (PostgreSQL-backed), `redis_client`, `s3_catalog` (MinIO-backed). The `iceberg_table` fixture creates a fresh table per test and drops it on teardown.

Scale fixtures (`scale_table_avg`, `scale_table_outlier`) write 20-point and 200-point communities once per session and share them across all scale tests — writing is slow, so they are not recreated per test.
