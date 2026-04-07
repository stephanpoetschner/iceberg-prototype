# Billing Archive Prototype

Validates the Iceberg/Parquet/DuckDB write-and-read pipeline for energy billing data using synthetic data. No Django dependency — runs standalone.

**Phase 1:** `num_points=1` throughout (14,880 records/test). Focus is a working pipeline with correct behaviour.
**Phase 2:** Scale tests and benchmarks (deferred — see `benchmarks/`).

## Architecture

```
prototype/
├── src/
│   ├── schema.py        # ICEBERG_SCHEMA, ARROW_SCHEMA, PARTITION_SPEC
│   ├── snapshot_id.py   # Deterministic make_snapshot_id(), SnapshotAlreadyExistsError
│   ├── synthetic.py     # Seeded record generator (14,880 records per community-month)
│   ├── writer.py        # write_community_snapshot(), write_snapshot() — atomic Iceberg append + sidecar
│   ├── sidecar.py       # BillingSnapshotMetadata Pydantic model, write/read JSON
│   └── reader.py        # DuckDB query helpers (all require snapshot_id)
├── tests/
│   ├── conftest.py      # catalog, iceberg_table, warehouse_dir, sidecar_dir fixtures
│   ├── test_schema.py
│   ├── test_snapshot_id.py
│   ├── test_synthetic.py
│   ├── test_write_read.py
│   ├── test_sidecar.py
│   ├── test_reader.py
│   ├── test_advanced.py  # null aggregation, concurrent writes
│   └── test_dst.py       # SNAP/NON-SNAP DST boundary correctness
├── benchmarks/           # Phase 2 standalone scripts (not pytest)
│   ├── bench_write.py
│   ├── bench_read.py
│   └── bench_memory.py
├── docker-compose.yml    # PostgreSQL 16 for Iceberg SQL catalog
└── pyproject.toml
```

## Prerequisites

- Python 3.12+
- Docker (for PostgreSQL)

## Setup

```bash
cd prototype

# 1. Start PostgreSQL (Iceberg SQL catalog)
docker compose up -d

# 2. Create and activate a virtual environment
python3.12 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -e ".[dev]"
```

Verify PostgreSQL is healthy before running tests:

```bash
docker compose ps   # STATUS should show "healthy"
```

## Running Tests

```bash
# All tests (slow collision test skipped by default — ~11s)
pytest

# Verbose output
pytest -v

# A specific test file
pytest tests/test_write_read.py -v

# Include the slow Phase 2 scale test (1.5M IDs, ~10–30s extra)
pytest -m slow
```

## Running Benchmarks (Phase 2)

Benchmarks are standalone scripts — not part of the test suite. Run them after `pytest` passes.

```bash
# Write throughput (1pt / 20pt avg / 200pt outlier, batch 10/100 communities)
python benchmarks/bench_write.py

# Read latency (all query patterns, 5-run average)
python benchmarks/bench_read.py

# Memory profiling — RSS delta (no extra tooling)
python benchmarks/bench_memory.py write 20
python benchmarks/bench_memory.py read 20

# Memory profiling — full memray flamegraph
memray run -o /tmp/write_20pts.bin benchmarks/bench_memory.py write 20
memray flamegraph /tmp/write_20pts.bin
```

Phase 2 success thresholds (from spec §11):
- Write: RSS delta ≤ ~300 MB for a 200-point community
- Read: sub-second latency for single-community queries
- Scale: partition count ≤ 30,000 at 2,500 communities × 12 months

## Key Design Decisions

**Idempotency via `snapshot_id`**
Each community export is identified by a deterministic `snapshot_id = make_snapshot_id(community_id, period_start, snapshot_version)`.

`write_community_snapshot()` checks for an existing row with that ID before writing and raises `SnapshotAlreadyExistsError` if found — its behavior is unchanged.

`write_snapshot()` is fully idempotent. It wraps `write_community_snapshot()` and handles three states:

| State | Trigger | Action |
|---|---|---|
| **First write** | `write_community_snapshot()` succeeds | Build metadata, write sidecar, return |
| **Clean retry** | `SnapshotAlreadyExistsError` + sidecar exists | Read and return existing sidecar |
| **Crash recovery** | `SnapshotAlreadyExistsError` + sidecar missing | Scan table for `record_count`, use `table.metadata_location`, write sidecar, return |

A retry with the same arguments always succeeds and returns a valid `BillingSnapshotMetadata`, regardless of which step a prior crash interrupted. Always filter queries with `WHERE snapshot_id = X` — without it, all versions of a community's data are visible together.

**Atomic writes**
`table.append()` in PyIceberg commits a single Iceberg snapshot. Readers see either all rows or none — no partial state.

**Partitioning**
`(community_id, time_month)` — keeps partition count bounded at scale and aligns with the primary query pattern.

**SNAP/NON-SNAP**
SNAP hours are 10:00–16:00 Europe/Vienna local time. DuckDB's `timezone('Europe/Vienna', time)` handles DST transitions correctly from stored UTC timestamps.

## Teardown

```bash
docker compose down        # stop PostgreSQL, keep data volume
docker compose down -v     # stop PostgreSQL and delete data volume
```
