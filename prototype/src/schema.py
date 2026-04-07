import pyarrow as pa
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, LongType, IntegerType, TimestamptzType, DecimalType,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform, MonthTransform


ICEBERG_SCHEMA = Schema(
    NestedField(field_id=1,  name="snapshot_id",        field_type=LongType(),          required=True),
    NestedField(field_id=2,  name="community_id",       field_type=IntegerType(),        required=True),
    NestedField(field_id=3,  name="time",               field_type=TimestamptzType(),    required=True),
    NestedField(field_id=4,  name="record_type",        field_type=IntegerType(),        required=True),
    NestedField(field_id=5,  name="ec_registration_id", field_type=LongType(),           required=True),
    NestedField(field_id=6,  name="metering_point_id",  field_type=LongType(),           required=True),
    NestedField(field_id=7,  name="value",              field_type=DecimalType(22, 6),   required=False),
    NestedField(field_id=8,  name="value_type",         field_type=IntegerType(),        required=True),
    NestedField(field_id=9,  name="message_created_at", field_type=TimestamptzType(),    required=True),
    NestedField(field_id=10, name="exported_at",        field_type=TimestamptzType(),    required=True),
    NestedField(field_id=11, name="snapshot_version",   field_type=IntegerType(),        required=True),
)

ARROW_SCHEMA = pa.schema([
    pa.field("snapshot_id",        pa.int64(),                   nullable=False),
    pa.field("community_id",       pa.int32(),                   nullable=False),
    pa.field("time",               pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("record_type",        pa.int32(),                   nullable=False),
    pa.field("ec_registration_id", pa.int64(),                   nullable=False),
    pa.field("metering_point_id",  pa.int64(),                   nullable=False),
    pa.field("value",              pa.decimal128(22, 6),         nullable=True),
    pa.field("value_type",         pa.int32(),                   nullable=False),
    pa.field("message_created_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("exported_at",        pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("snapshot_version",   pa.int32(),                   nullable=False),
])

PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=2, field_id=100, transform=IdentityTransform(), name="community_id"),
    PartitionField(source_id=3, field_id=101, transform=MonthTransform(),    name="time_month"),
)
