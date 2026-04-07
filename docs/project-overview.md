# Billing Archive — Project Overview

**Status**: Planning
**Date**: 2026-03-26
**Team**: EDM

---

## Context

EDM operates an energy data management platform that ingests 15-minute smart meter readings for approximately 50,000 metering points across roughly 2,500 energy communities in Austria. All metering data currently lives in TimescaleDB (PostgreSQL), and billing exports are pre-aggregated CSV files.

This architecture was appropriate for flat-tariff billing but cannot support the company's next phase of growth.

---

## Problems — Priority Order

### P1: Dynamic tariff billing requires granular data access

A feature request for fully dynamic tariffs — where every 15-minute slot may carry a different price — means the billing pipeline needs access to every individual quarter-hour record, not just monthly aggregates.

Today's billing export produces CSV files containing only aggregated sums. There is no mechanism to deliver frozen, verifiable, per-period datasets at quarter-hour granularity to the billing pipeline.

**Impact**: Blocks a key product feature. The billing pipeline redesign cannot proceed without a granular data source.

### P2: Unbounded growth of the operational database

All historical metering data remains in TimescaleDB indefinitely — including fully settled billing periods that will never be recomputed. At approximately 744 million new records per month, this degrades query performance over time and costs significantly more than cold storage.

**Impact**: Increasing operational cost and degrading application performance. Not yet critical, but worsens every month.

### P3: Analytical and third-party data access

The company wants to develop energy consumption and production forecasting models. This analytical work has different performance constraints than the web application and should not run against the production database. External partners specializing in modeling may also need access to historical data without being granted credentials to the main database.

**Impact**: Blocks the company's data analytics maturity. Currently no way to share historical metering data safely outside the production system.

---

## Solution Direction

An archival pipeline writes frozen, per-community, per-billing-period snapshots of all quarter-hour metering records into Apache Iceberg tables backed by Parquet files on S3.

Downstream consumers — the redesigned billing pipeline, analytics workloads, and third-party partners — read data via DuckDB using only S3 credentials and a metadata URI. No database access or catalog service is required.

### Why Iceberg + Parquet + DuckDB

| Concern | How this stack addresses it |
|---|---|
| Verifiable snapshots | Iceberg provides immutable table snapshots; deterministic `snapshot_id` gives the billing pipeline a stable reference for cryptographic sealing at invoice time |
| Granular access | Quarter-hour records stored individually, partitioned by community and month |
| Decoupled read access | DuckDB reads Parquet/Iceberg directly from S3 — no shared database, no catalog dependency |
| Long-term readability | Parquet is an open columnar format readable by any analytics tool; viable for 10+ year retention |
| Storage cost | Compressed Parquet on S3 costs ~$0.20/month for all communities — orders of magnitude cheaper than PostgreSQL |

---

## Approach: Prototype First

Before modifying the production system, a standalone prototype will demonstrate the full write-and-read cycle using synthetic data at realistic scale (50,000 metering points, ~744M records/month).

The prototype will:

- Generate synthetic metering data matching production characteristics
- Write data into Iceberg via PyIceberg with the proposed schema and partitioning
- Use PostgreSQL as the Iceberg catalog (matching production — not SQLite)
- Read and query data via DuckDB without catalog access
- Validate atomic writes: each community export is a single Iceberg commit, invisible to readers until complete
- Validate idempotency: `write_snapshot()` is fully idempotent — retries recover from a crash between the Iceberg commit and the sidecar write; duplicate exports are detected and rejected at the Iceberg level
- Validate concurrent writes: multiple processes writing different communities simultaneously
- Run performance benchmarks for both write and read paths, including DST transition correctness
- Validate the metadata sidecar design (business context and discovery; cryptographic sealing deferred to billing pipeline)

The prototype validates the technical approach and surfaces scaling issues before committing to the architecture in production.

---

## What Comes After the Prototype

These are explicitly **out of scope** for the prototype but represent the path forward:

1. **Production pipeline integration** — Celery task replacing the Jupyter notebook, wired into the existing Django application
2. **Billing pipeline redesign** — New consumer that reads granular data from the archive for dynamic tariff invoicing
3. **Historical data offload** — Once the archive is trusted as the system of record, retire settled periods from TimescaleDB
4. **Analytics access** — Read-only S3 credentials and DuckDB query patterns for forecasting teams and external partners
