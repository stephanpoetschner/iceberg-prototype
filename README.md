# Billing Archive

Archival pipeline for Austrian energy billing data. Writes frozen, per-community, per-billing-period snapshots of 15-minute smart meter records into Apache Iceberg tables on S3, queryable via DuckDB without catalog access.

**Stack**: PyIceberg · PyArrow · DuckDB · PostgreSQL (Iceberg catalog) · Celery

## Documentation

- [Project overview and motivation](docs/overview.md)
- [Production specification](docs/spec.md)
- [Reference material](docs/reference/)

## Getting Started

See [prototype/README.md](prototype/README.md).
