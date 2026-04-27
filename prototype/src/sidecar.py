import json
from datetime import datetime
from pydantic import BaseModel, Field


class MeteringPointRecord(BaseModel):
    id: int
    external_id: str
    name: str
    energy_direction: int  # 1=import, 2=export


class ECRegistrationRecord(BaseModel):
    id: int
    external_id: str
    meteringpoint_id: int
    community_id: int
    registered_from: datetime
    registered_until: datetime | None


class BillingSnapshotMetadata(BaseModel):
    snapshot_id: int
    snapshot_version: int           # 1=original, 2+=correction
    community_id: int
    community_name: str
    community_ec_id: str
    period_start: datetime
    period_end: datetime
    record_count: int
    exported_at: datetime
    iceberg_metadata_uri: str       # pointer to metadata file at time of write
    ec_registrations: list[ECRegistrationRecord] = Field(min_length=1)
    metering_points: list[MeteringPointRecord] = Field(min_length=1)


def write_sidecar(path: str, metadata: BillingSnapshotMetadata) -> None:
    """Serialize BillingSnapshotMetadata to JSON at the given path."""
    with open(path, "w") as f:
        f.write(metadata.model_dump_json(indent=2))


def read_sidecar(path: str) -> BillingSnapshotMetadata:
    """Deserialize BillingSnapshotMetadata from a JSON file."""
    with open(path) as f:
        return BillingSnapshotMetadata.model_validate_json(f.read())
