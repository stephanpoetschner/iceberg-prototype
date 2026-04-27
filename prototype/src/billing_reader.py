# prototype/src/billing_reader.py
import pyarrow as pa
from reader import _conn, _s3_client_from_config
from sidecar import BillingSnapshotMetadata, read_sidecar


def _load_sidecar_s3(sidecar_uri: str, s3_config: dict | None = None) -> BillingSnapshotMetadata:
    import boto3
    without_scheme = sidecar_uri[len("s3://"):]
    bucket, *key_parts = without_scheme.split("/")
    key = "/".join(key_parts)
    client = _s3_client_from_config(s3_config) if s3_config else boto3.client("s3")
    obj = client.get_object(Bucket=bucket, Key=key)
    return BillingSnapshotMetadata.model_validate_json(obj["Body"].read())


class BillingReader:
    """Read-side entrypoint for one billing snapshot.

    Uses sidecar.iceberg_metadata_uri verbatim — never auto-resolves to latest.
    """

    def __init__(self, sidecar: BillingSnapshotMetadata, s3_config: dict | None = None):
        self.sidecar = sidecar
        self._s3_config = s3_config

    @classmethod
    def from_sidecar_uri(
        cls, sidecar_uri: str, s3_config: dict | None = None
    ) -> "BillingReader":
        if sidecar_uri.startswith("file://"):
            sidecar = read_sidecar(sidecar_uri[len("file://"):])
        elif sidecar_uri.startswith("s3://"):
            sidecar = _load_sidecar_s3(sidecar_uri, s3_config)
        else:
            raise ValueError(f"Unsupported URI scheme: {sidecar_uri!r}")
        return cls(sidecar=sidecar, s3_config=s3_config)

    def community_billing(self) -> pa.Table:
        uri = self.sidecar.iceberg_metadata_uri
        cid = self.sidecar.community_id
        sid = self.sidecar.snapshot_id
        conn = _conn(self._s3_config)
        return conn.execute(
            f"""
            SELECT *
            FROM iceberg_scan('{uri}')
            WHERE community_id = {cid}
              AND snapshot_id  = {sid}
            """
        ).arrow().read_all()

    def snap_nonsnap(self) -> pa.Table:
        uri = self.sidecar.iceberg_metadata_uri
        cid = self.sidecar.community_id
        sid = self.sidecar.snapshot_id
        conn = _conn(self._s3_config)
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
            WHERE community_id = {cid}
              AND snapshot_id  = {sid}
            GROUP BY 1, 2
            """
        ).arrow().read_all()

    def registration_detail(self, registration_id: int) -> pa.Table:
        uri = self.sidecar.iceberg_metadata_uri
        cid = self.sidecar.community_id
        sid = self.sidecar.snapshot_id
        conn = _conn(self._s3_config)
        return conn.execute(
            f"""
            SELECT time, record_type, value, value_type
            FROM iceberg_scan('{uri}')
            WHERE community_id       = {cid}
              AND snapshot_id        = {sid}
              AND ec_registration_id = {registration_id}
            ORDER BY time
            """
        ).arrow().read_all()
