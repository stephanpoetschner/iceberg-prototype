# CLAUDE.md

## What This Project Is

Prototype validating an Apache Iceberg + Parquet + DuckDB archival pipeline for Austrian energy billing data. Proves the write-and-read cycle with synthetic data before integration into a Django/Celery production app. Production target: archive 15-min metering records for ~50,000 metering points across ~2,500 communities into Iceberg tables on S3.

- `SPEC.md` — full production specification
- `docs/project-overview.md` — business context and what comes after the prototype

## Living Documents — Keep Updated

- `docs/project-overview.md` — business context and next steps
- `docs/superpowers/plans/2026-03-26-billing-archive-prototype.md` — Phase 1 plan; update File Map when modules change

## Running Tests

All prototype work is in `prototype/`. Docker services and venv are required (see `agent_docs/development-setup.md`).

```bash
cd prototype
pytest                          # fast suite (<30s, slow tests excluded)
pytest -m slow                  # include scale tests
pytest tests/test_write_read.py::test_write_and_read -v  # single test
```

Tests require all three Docker services healthy (`docker compose ps`). Redis tests: `@pytest.mark.redis`. S3/MinIO tests: `@pytest.mark.s3`.

## Key Design Invariants

**Snapshot ID is deterministic.** `make_snapshot_id(community_id, period_start, snapshot_version)` always returns the same integer — enables idempotency. `write_community_snapshot()` raises `SnapshotAlreadyExistsError` if data for those inputs is already committed.

**Every query must filter by `snapshot_id`.** Partition is `(community_id, month(time))` — multiple snapshot versions share a partition directory. Omitting the filter silently mixes versions.

**Two-phase write: Iceberg commit, then sidecar.** A crash between them leaves committed data with no sidecar. Recovery: re-run `write_snapshot()`, which detects the committed data via `SnapshotAlreadyExistsError` and writes the missing sidecar. See `docs/superpowers/specs/2026-03-27-write-snapshot-idempotency-design.md`.

**Iceberg and PyArrow schemas must match exactly.** `LongType()→pa.int64()`, `IntegerType()→pa.int32()`, `TimestamptzType()→pa.timestamp("us","UTC")`. Mismatches cause silent data corruption.

**SNAP hours use Europe/Vienna local time.** Timestamps stored as UTC. DuckDB queries must use `timezone('Europe/Vienna', time)` for SNAP (10:00–16:00) vs NON-SNAP windows around DST transitions.

## Architecture Reference

See `agent_docs/architecture.md` for module layout, storage structure, Docker services, and test fixtures.
