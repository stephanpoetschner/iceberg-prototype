# src/reader.py
import os
import duckdb
import pyarrow as pa


def _conn(s3_config: dict | None = None) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with the Iceberg extension loaded.

    s3_config dict keys (all optional if not using S3):
        endpoint   — e.g. "http://localhost:9000" (for MinIO) or omit for AWS
        access_key — AWS access key ID
        secret_key — AWS secret access key
        use_ssl    — bool, default True; set False for local MinIO
    """
    conn = duckdb.connect()
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    if s3_config:
        if endpoint := s3_config.get("endpoint"):
            # Strip scheme for DuckDB; DuckDB's s3_endpoint is hostname[:port] only
            host = endpoint.replace("http://", "").replace("https://", "")
            conn.execute(f"SET s3_endpoint='{host}'")
        if key := s3_config.get("access_key"):
            conn.execute(f"SET s3_access_key_id='{key}'")
        if secret := s3_config.get("secret_key"):
            conn.execute(f"SET s3_secret_access_key='{secret}'")
        if not s3_config.get("use_ssl", True):
            conn.execute("SET s3_use_ssl=false")
            conn.execute("SET s3_url_style='path'")  # MinIO requires path-style
    return conn


def _metadata_sort_key(filename: str) -> int:
    """Sort key for Iceberg metadata filenames.

    Handles two naming conventions:
        vN.metadata.json         — numeric version prefix with 'v'
        NNNNN-uuid.metadata.json — zero-padded integer before first '-'
    """
    prefix = filename.split('.')[0]
    stripped = prefix.lstrip('v')
    if stripped.isdigit():
        return int(stripped)
    leading = stripped.split('-')[0]
    if leading.isdigit():
        return int(leading)
    return hash(filename)  # fallback: unrecognised format sorts last


def _s3_client_from_config(s3_config: dict):
    """Build a boto3 S3 client from the s3_config dict used by this module."""
    import boto3
    kwargs = {}
    if endpoint := s3_config.get("endpoint"):
        kwargs["endpoint_url"] = endpoint
    if key := s3_config.get("access_key"):
        kwargs["aws_access_key_id"] = key
    if secret := s3_config.get("secret_key"):
        kwargs["aws_secret_access_key"] = secret
    return boto3.client("s3", **kwargs)


def _resolve_latest_metadata(metadata_uri: str, s3_config: dict | None = None) -> str:
    """Given any metadata.json URI for an Iceberg table, return the URI of the
    latest metadata file in the same directory.

    Supports file:// and s3:// schemes. For s3://, requires boto3 and valid
    AWS credentials (or MinIO config via environment variables or explicit config).
    """
    if metadata_uri.startswith("file://"):
        return _resolve_latest_metadata_local(metadata_uri)
    if metadata_uri.startswith("s3://"):
        s3_client = _s3_client_from_config(s3_config) if s3_config else None
        return _resolve_latest_metadata_s3(metadata_uri, s3_client=s3_client)
    raise ValueError(f"Unsupported URI scheme: {metadata_uri!r}")


def _resolve_latest_metadata_local(metadata_uri: str) -> str:
    local_path = metadata_uri.replace("file://", "")
    meta_dir = os.path.dirname(local_path)
    files = sorted(
        (f for f in os.listdir(meta_dir) if f.endswith(".metadata.json")),
        key=_metadata_sort_key,
    )
    if not files:
        return metadata_uri
    return f"file://{meta_dir}/{files[-1]}"


def _resolve_latest_metadata_s3(metadata_uri: str, s3_client=None) -> str:
    """Resolve the latest metadata file for an s3:// Iceberg URI.

    Uses boto3 to list objects in the metadata directory. Credentials are
    picked up from the environment (AWS_ACCESS_KEY_ID etc.) or the s3_client
    argument if provided (useful for MinIO endpoint config).
    """
    import boto3

    # Parse s3://bucket/path/to/table/metadata/NNNNN-uuid.metadata.json
    without_scheme = metadata_uri[len("s3://"):]
    bucket, *key_parts = without_scheme.split("/")
    key = "/".join(key_parts)
    meta_dir = "/".join(key.split("/")[:-1])  # strip the filename

    if s3_client is None:
        s3_client = boto3.client("s3")

    paginator = s3_client.get_paginator("list_objects_v2")
    filenames = []
    for page in paginator.paginate(Bucket=bucket, Prefix=meta_dir + "/"):
        for obj in page.get("Contents", []):
            name = obj["Key"].split("/")[-1]
            if name.endswith(".metadata.json"):
                filenames.append(name)

    if not filenames:
        return metadata_uri

    latest = sorted(filenames, key=_metadata_sort_key)[-1]
    return f"s3://{bucket}/{meta_dir}/{latest}"


def query_community_billing(
    metadata_uri: str, community_id: int, snapshot_id: int, s3_config: dict | None = None
) -> pa.Table:
    """Retrieve all records for a specific community + snapshot. snapshot_id is mandatory."""
    uri = _resolve_latest_metadata(metadata_uri, s3_config=s3_config)
    conn = _conn(s3_config)
    return conn.execute(
        f"""
        SELECT *
        FROM iceberg_scan('{uri}')
        WHERE community_id = {community_id}
          AND snapshot_id  = {snapshot_id}
        """
    ).arrow().read_all()


def query_snap_nonsnap(
    metadata_uri: str, community_id: int, snapshot_id: int, s3_config: dict | None = None
) -> pa.Table:
    """SNAP (10:00–16:00 Vienna local time) vs NON-SNAP aggregation per registration and record type."""
    uri = _resolve_latest_metadata(metadata_uri, s3_config=s3_config)
    conn = _conn(s3_config)
    return conn.execute(
        f"""
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
        FROM iceberg_scan('{uri}')
        WHERE community_id = {community_id}
          AND snapshot_id  = {snapshot_id}
        GROUP BY 1, 2
        """
    ).arrow().read_all()


def query_registration_detail(
    metadata_uri: str, community_id: int, snapshot_id: int, registration_id: int,
    s3_config: dict | None = None,
) -> pa.Table:
    """Full quarter-hour detail for one registration, ordered by time."""
    uri = _resolve_latest_metadata(metadata_uri, s3_config=s3_config)
    conn = _conn(s3_config)
    return conn.execute(
        f"""
        SELECT time, record_type, value, value_type
        FROM iceberg_scan('{uri}')
        WHERE community_id       = {community_id}
          AND snapshot_id        = {snapshot_id}
          AND ec_registration_id = {registration_id}
        ORDER BY time
        """
    ).arrow().read_all()


def query_cross_community(
    metadata_uri: str, snapshot_ids: list[int], s3_config: dict | None = None
) -> pa.Table:
    """Sum across multiple communities for analytics."""
    uri = _resolve_latest_metadata(metadata_uri, s3_config=s3_config)
    ids_sql = ", ".join(str(i) for i in snapshot_ids)
    conn = _conn(s3_config)
    return conn.execute(
        f"""
        SELECT
            community_id,
            record_type,
            SUM(value) AS total
        FROM iceberg_scan('{uri}')
        WHERE snapshot_id IN ({ids_sql})
        GROUP BY 1, 2
        """
    ).arrow().read_all()
