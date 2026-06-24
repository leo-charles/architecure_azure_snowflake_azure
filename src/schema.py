from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, TimestamptzType, DateType,
    IntegerType, BooleanType, ListType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import IdentityTransform


SILVER_TRAYS_SCHEMA = Schema(
    NestedField(1,  "tray_id",                  StringType(),      required=True),
    NestedField(2,  "machine_id",               StringType(),      required=True),
    NestedField(3,  "candled_at",               TimestamptzType(), required=True),
    NestedField(4,  "candled_date",             DateType(),        required=True),
    NestedField(5,  "flock",                    IntegerType(),     required=True),
    NestedField(6,  "trolley",                  StringType(),      required=True),
    NestedField(7,  "tray_seq",                 IntegerType(),     required=True),
    NestedField(8,  "flock_name",               StringType(),      required=False),
    NestedField(9,  "trolley_name",             StringType(),      required=False),
    NestedField(10, "caliber",                  StringType(),      required=False),
    NestedField(11, "setter_tray_number_flock", IntegerType(),     required=False),
    NestedField(12, "n_total",                  IntegerType(),     required=True),
    NestedField(13, "n_fertile",                IntegerType(),     required=True),
    NestedField(14, "n_clear",                  IntegerType(),     required=True),
    NestedField(15, "n_early_dead",             IntegerType(),     required=True),
    NestedField(16, "n_late_dead",              IntegerType(),     required=True),
    NestedField(17, "n_missing",                IntegerType(),     required=True),
    NestedField(18, "is_count_consistent",      BooleanType(),     required=True),
    NestedField(19, "matrix_compact",           StringType(),      required=True),
    NestedField(20, "light_flat",               ListType(21, IntegerType(), element_required=False), required=False),
    NestedField(22, "alarm_emergency_stop",     IntegerType(),     required=False),
    NestedField(23, "alarm_air_pressure_fault", IntegerType(),     required=False),
    NestedField(24, "processed_at",             TimestamptzType(), required=True),
    NestedField(25, "bronze_path",              StringType(),      required=False),
    NestedField(26, "year",                     IntegerType(),     required=True),
    NestedField(27, "month",                    IntegerType(),     required=True),
    NestedField(28, "day",                      IntegerType(),     required=True),
)

SILVER_TRAYS_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=2,  field_id=1001, transform=IdentityTransform(), name="machine_id"),
    PartitionField(source_id=26, field_id=1002, transform=IdentityTransform(), name="year"),
    PartitionField(source_id=27, field_id=1003, transform=IdentityTransform(), name="month"),
    PartitionField(source_id=28, field_id=1004, transform=IdentityTransform(), name="day"),
)


def get_or_create_silver_trays(catalog, adls_uri: str):
    """
    Charge la table silver.trays si elle existe,
    la crée sinon. Retourne la table dans tous les cas.
    """
    try:
        catalog.create_namespace("silver")
    except Exception:
        pass

    existing = [t for t in catalog.list_tables("silver") if t == ("silver", "trays")]

    if existing:
        table = catalog.load_table("silver.trays")
        print(f"Table 'silver.trays' chargée — {len(table.snapshots())} snapshots.")
    else:
        table = catalog.create_table(
            identifier="silver.trays",
            schema=SILVER_TRAYS_SCHEMA,
            partition_spec=SILVER_TRAYS_PARTITION_SPEC,
            location=f"{adls_uri}/trays_iceberg",
        )
        print("Table 'silver.trays' créée.")

    print(f"  Location : {table.location()}")
    print(f"  Format   : Iceberg v{table.format_version}")
    print(f"  Champs   : {len(table.schema().fields)}")
    return table