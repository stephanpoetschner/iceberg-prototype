# prototype/tests/conftest.py
import os
import pytest
import uuid
from pyiceberg.catalog import load_catalog
from schema import ICEBERG_SCHEMA, PARTITION_SPEC

CATALOG_URI = "postgresql+psycopg2://iceberg:iceberg@localhost:5432/iceberg_test"
NAMESPACE   = "billing_test"


@pytest.fixture(scope="session")
def warehouse_dir(tmp_path_factory):
    return str(tmp_path_factory.mktemp("warehouse"))


@pytest.fixture(scope="session")
def catalog(warehouse_dir):
    cat = load_catalog("test", **{
        "type":      "sql",
        "uri":       CATALOG_URI,
        "warehouse": f"file://{warehouse_dir}",
    })
    try:
        cat.create_namespace(NAMESPACE)
    except Exception:
        pass  # namespace already exists
    return cat


@pytest.fixture
def iceberg_table(catalog):
    table_ident = f"{NAMESPACE}.records_{uuid.uuid4().hex[:8]}"
    table = catalog.create_table(
        table_ident,
        schema=ICEBERG_SCHEMA,
        partition_spec=PARTITION_SPEC,
    )
    yield table
    catalog.drop_table(table_ident)


@pytest.fixture
def sidecar_dir(warehouse_dir):
    path = os.path.join(warehouse_dir, "sidecars")
    os.makedirs(path, exist_ok=True)
    return path


import redis as redis_lib

REDIS_URL = "redis://localhost:6379/0"


@pytest.fixture(scope="session")
def redis_client():
    client = redis_lib.from_url(REDIS_URL, decode_responses=False)
    client.ping()  # fail fast if Redis is not up
    yield client
    client.close()


def pytest_configure(config):
    config.addinivalue_line("markers", "redis: tests that require a running Redis instance")
    config.addinivalue_line("markers", "s3: tests that require a running MinIO/S3 instance")


# ── Phase 2 scale fixtures ─────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def scale_table_avg(catalog, warehouse_dir):
    """20-point average community — written once, shared across all avg-scale tests."""
    from datetime import datetime, timezone
    from schema import ICEBERG_SCHEMA, PARTITION_SPEC
    from snapshot_id import make_snapshot_id
    from synthetic import generate_records
    from writer import write_community_snapshot
    import uuid

    period      = datetime(2026, 1, 1, tzinfo=timezone.utc)
    exported_at = datetime(2026, 3, 26, 12, 0, 0, tzinfo=timezone.utc)

    ident   = f"{NAMESPACE}.scale_avg_{uuid.uuid4().hex[:8]}"
    table   = catalog.create_table(ident, schema=ICEBERG_SCHEMA, partition_spec=PARTITION_SPEC)
    records = generate_records(community_id=500, period_start=period, num_points=20, seed=500)
    sid     = make_snapshot_id(500, period, 1)
    uri, _  = write_community_snapshot(table, records, snapshot_id=sid, exported_at=exported_at)

    yield {
        "table": table, "uri": uri, "sid": sid,
        "records": records, "num_points": 20,
        "community_id": 500, "period": period,
    }
    catalog.drop_table(ident)


@pytest.fixture(scope="session")
def scale_table_outlier(catalog, warehouse_dir):
    """200-point outlier community — written once, shared across all outlier-scale tests."""
    from datetime import datetime, timezone
    from schema import ICEBERG_SCHEMA, PARTITION_SPEC
    from snapshot_id import make_snapshot_id
    from synthetic import generate_records
    from writer import write_community_snapshot
    import uuid

    period      = datetime(2026, 1, 1, tzinfo=timezone.utc)
    exported_at = datetime(2026, 3, 26, 12, 0, 0, tzinfo=timezone.utc)

    ident   = f"{NAMESPACE}.scale_outlier_{uuid.uuid4().hex[:8]}"
    table   = catalog.create_table(ident, schema=ICEBERG_SCHEMA, partition_spec=PARTITION_SPEC)
    records = generate_records(community_id=600, period_start=period, num_points=200, seed=600)
    sid     = make_snapshot_id(600, period, 1)
    uri, _  = write_community_snapshot(table, records, snapshot_id=sid, exported_at=exported_at)

    yield {
        "table": table, "uri": uri, "sid": sid,
        "records": records, "num_points": 200,
        "community_id": 600, "period": period,
    }
    catalog.drop_table(ident)


import boto3 as boto3_lib

S3_CONFIG = {
    "endpoint":   "http://localhost:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
    "use_ssl":    False,
}
S3_BUCKET = "iceberg-test"


def _s3_client():
    return boto3_lib.client(
        "s3",
        endpoint_url=S3_CONFIG["endpoint"],
        aws_access_key_id=S3_CONFIG["access_key"],
        aws_secret_access_key=S3_CONFIG["secret_key"],
    )


@pytest.fixture(scope="session")
def s3_catalog():
    """PyIceberg catalog backed by PostgreSQL metadata + MinIO data."""
    s3 = _s3_client()
    try:
        s3.create_bucket(Bucket=S3_BUCKET)
    except Exception:
        pass  # bucket already exists
    cat = load_catalog("test_s3", **{
        "type":                  "sql",
        "uri":                   CATALOG_URI,
        "warehouse":             f"s3://{S3_BUCKET}/warehouse",
        "s3.endpoint":           S3_CONFIG["endpoint"],
        "s3.access-key-id":      S3_CONFIG["access_key"],
        "s3.secret-access-key":  S3_CONFIG["secret_key"],
        "s3.path-style-access":  "true",
    })
    try:
        cat.create_namespace("billing_test_s3")
    except Exception:
        pass
    return cat


@pytest.fixture
def s3_iceberg_table(s3_catalog):
    table_ident = f"billing_test_s3.records_{uuid.uuid4().hex[:8]}"
    table = s3_catalog.create_table(
        table_ident,
        schema=ICEBERG_SCHEMA,
        partition_spec=PARTITION_SPEC,
    )
    yield table
    s3_catalog.drop_table(table_ident)
