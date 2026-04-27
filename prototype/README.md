# Billing Archive Prototype

Validates the Iceberg/Parquet/DuckDB write-and-read pipeline for energy billing data using synthetic data. No Django dependency — runs standalone.

## Architecture

```
prototype/
├── src/
│   ├── schema.py        # ICEBERG_SCHEMA, ARROW_SCHEMA, PARTITION_SPEC
│   ├── snapshot_id.py   # Deterministic make_snapshot_id(), SnapshotAlreadyExistsError
│   ├── synthetic.py     # Seeded record generator (14,880 records per community-month)
│   ├── writer.py        # write_community_snapshot(), write_snapshot() — atomic Iceberg append + sidecar
│   ├── sidecar.py       # BillingSnapshotMetadata Pydantic model, write/read JSON
│   ├── reader.py        # DuckDB query helpers (all require snapshot_id)
│   └── lock.py          # Redis-backed snapshot_lock() context manager for concurrent-write safety
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
├── benchmarks/
│   ├── bench_write.py
│   ├── bench_read.py
│   └── bench_memory.py
├── docker-compose.yml    # PostgreSQL, Redis, MinIO
└── pyproject.toml
```

## Prerequisites

- Python 3.12+
- Docker (for PostgreSQL, Redis, and MinIO)

## Setup

```bash
cd prototype

# 1. Start PostgreSQL (Iceberg SQL catalog), Redis (distributed locks), MinIO (S3-compatible storage)
docker compose up -d

# 2. Create and activate a virtual environment
python3.12 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -e ".[dev]"
```

Verify services are healthy before running tests:

```bash
docker compose ps   # STATUS should show "healthy"
```

## Docker Services

| Service | Port | Purpose |
|---|---|---|
| PostgreSQL 16 | 5432 | Iceberg SQL catalog (`iceberg/iceberg/iceberg_test`) |
| Redis 7 | 6379 | Distributed lock for concurrent snapshot writes |
| MinIO | 9000 / 9001 | S3-compatible storage for S3 integration tests |

## Running Tests

```bash
pytest              # fast suite (<30s, slow tests excluded)
pytest -v           # verbose output
pytest -m slow      # include scale tests (~10–30s extra)
pytest tests/test_write_read.py -v  # single file
```

Redis tests: `@pytest.mark.redis`. S3/MinIO tests: `@pytest.mark.s3`.

## Running Benchmarks

Benchmarks are standalone scripts — not part of the test suite:

```bash
python benchmarks/bench_write.py
python benchmarks/bench_read.py

# Memory profiling — RSS delta
python benchmarks/bench_memory.py write 20
python benchmarks/bench_memory.py read 20

# Memory profiling — full memray flamegraph
memray run -o /tmp/write_20pts.bin benchmarks/bench_memory.py write 20
memray flamegraph /tmp/write_20pts.bin
```

Success thresholds:
- Write: RSS delta ≤ ~300 MB for a 200-point community
- Read: sub-second latency for single-community queries
- Scale: partition count ≤ 30,000 at 2,500 communities × 12 months

## Key Design Decisions

**Idempotency via `snapshot_id`**
Each export is identified by `make_snapshot_id(community_id, period_start, snapshot_version)` — deterministic, same inputs always produce the same ID. `write_snapshot()` is fully idempotent: a retry with the same arguments always succeeds, recovering from a crash between the Iceberg commit and the sidecar write. See [Design Decisions §9](../docs/reference/design-decisions.md#9-idempotency-two-level-guard).

**Atomic writes**
`table.append()` commits a single Iceberg snapshot — readers see either all rows or none.

**Partitioning**
`(community_id, time_month)` — bounds partition count at scale and aligns with the primary query pattern. Always filter queries with `WHERE snapshot_id = X`; without it, all versions are visible together.

**SNAP/NON-SNAP**
SNAP hours are 10:00–16:00 Europe/Vienna local time. DuckDB's `timezone('Europe/Vienna', time)` handles DST transitions correctly from stored UTC timestamps.

## Teardown

```bash
docker compose down     # stop services, keep data volume
docker compose down -v  # stop services and delete data volume
```
