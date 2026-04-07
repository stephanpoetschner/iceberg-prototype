# prototype/tests/test_schema.py
import pyarrow as pa
from schema import ICEBERG_SCHEMA, ARROW_SCHEMA, PARTITION_SPEC
from pyiceberg.types import (
    LongType, IntegerType, TimestamptzType, DecimalType
)


def test_iceberg_schema_field_count():
    assert len(ICEBERG_SCHEMA.fields) == 11


def test_iceberg_schema_field_types():
    fields = {f.name: f for f in ICEBERG_SCHEMA.fields}
    assert isinstance(fields["snapshot_id"].field_type, LongType)
    assert isinstance(fields["community_id"].field_type, IntegerType)
    assert isinstance(fields["time"].field_type, TimestamptzType)
    assert isinstance(fields["record_type"].field_type, IntegerType)
    assert isinstance(fields["ec_registration_id"].field_type, LongType)
    assert isinstance(fields["metering_point_id"].field_type, LongType)
    assert isinstance(fields["value"].field_type, DecimalType)
    assert fields["value"].field_type.precision == 22
    assert fields["value"].field_type.scale == 6
    assert fields["value"].optional is True  # nullable
    assert isinstance(fields["value_type"].field_type, IntegerType)
    assert isinstance(fields["message_created_at"].field_type, TimestamptzType)
    assert isinstance(fields["exported_at"].field_type, TimestamptzType)
    assert isinstance(fields["snapshot_version"].field_type, IntegerType)
    assert fields["snapshot_version"].optional is False


def test_arrow_schema_field_count():
    assert len(ARROW_SCHEMA) == 11


def test_arrow_schema_field_types():
    f = {field.name: field for field in ARROW_SCHEMA}
    assert f["snapshot_id"].type == pa.int64()
    assert f["community_id"].type == pa.int32()
    assert f["time"].type == pa.timestamp("us", tz="UTC")
    assert f["record_type"].type == pa.int32()
    assert f["ec_registration_id"].type == pa.int64()
    assert f["metering_point_id"].type == pa.int64()
    assert f["value"].type == pa.decimal128(22, 6)
    assert f["value"].nullable is True
    assert f["value_type"].type == pa.int32()
    assert f["message_created_at"].type == pa.timestamp("us", tz="UTC")
    assert f["exported_at"].type == pa.timestamp("us", tz="UTC")
    assert f["snapshot_version"].type == pa.int32()
    assert f["snapshot_version"].nullable is False


def test_partition_spec_fields():
    fields = {f.name: f for f in PARTITION_SPEC.fields}
    assert "community_id" in fields
    assert "time_month" in fields
