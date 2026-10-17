from pyspark.sql.types import StructField, StructType

from pyspark_contracts._field import Field


def fields_to_struct_type(fields: dict[str, Field]) -> StructType:
    """The Spark schema a set of contract fields declares, in declaration order.

    Each ``Field``'s ``description`` becomes the column's ``comment`` metadata, which Spark
    uses as the column comment when the DataFrame is saved as a table.
    """
    return StructType(
        [
            StructField(
                name,
                field.dtype,
                field.nullable,
                {"comment": field.description} if field.description else {},
            )
            for name, field in fields.items()
        ]
    )
