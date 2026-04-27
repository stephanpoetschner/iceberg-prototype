# CLAUDE.md

## What This Project Is

Prototype validating an Apache Iceberg + Parquet + DuckDB archival pipeline for Austrian energy billing data. Production target: archive 15-min metering records for ~50,000 metering points across ~300 communities into Iceberg tables on S3.

- [Project overview](docs/overview.md)
- [Production specification](docs/spec.md)
- [Design decisions](docs/reference/design-decisions.md)

## Running Tests

```bash
cd prototype
pytest                 # fast suite (<30s)
pytest -m slow         # include scale tests
```

Tests require all three Docker services healthy (`docker compose ps`). Redis tests: `@pytest.mark.redis`. S3/MinIO tests: `@pytest.mark.s3`.

## Key Design Invariants

**Snapshot ID is deterministic.** `make_snapshot_id(community_id, period_start, snapshot_version)` always returns the same integer — enables idempotency. `write_community_snapshot()` raises `SnapshotAlreadyExistsError` if data for those inputs is already committed.

**Every query must filter by `snapshot_id`.** Partition is `(community_id, month(time))` — multiple snapshot versions share a partition directory. Omitting the filter silently mixes versions.

**Two-phase write: Iceberg commit, then sidecar.** A crash between them leaves committed data with no sidecar. Recovery: re-run `write_snapshot()`, which detects the committed data via `SnapshotAlreadyExistsError` and writes the missing sidecar. See [Design Decisions §9](docs/reference/design-decisions.md#9-idempotency-two-level-guard).

**Iceberg and PyArrow schemas must match exactly.** `LongType()→pa.int64()`, `IntegerType()→pa.int32()`, `TimestamptzType()→pa.timestamp("us","UTC")`. Mismatches cause silent data corruption.

**SNAP hours use Europe/Vienna local time.** Timestamps stored as UTC. DuckDB queries must use `timezone('Europe/Vienna', time)` for SNAP (10:00–16:00) vs NON-SNAP windows around DST transitions.
