from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, DataType, MapType, StructField, StructType

from pyspark_contracts._report import Violation, ViolationKind


def _without_metadata(dtype: DataType) -> DataType:
    """``dtype`` with the metadata of every nested ``StructField`` dropped.

    Field metadata is documentation (comments, units, ...), and Spark compares it as part of
    type equality, so a struct read from a table with column comments would otherwise never
    match the contract's.
    """
    if isinstance(dtype, StructType):
        return StructType(
            [StructField(f.name, _without_metadata(f.dataType), f.nullable) for f in dtype.fields]
        )
    if isinstance(dtype, ArrayType):
        return ArrayType(_without_metadata(dtype.elementType), dtype.containsNull)
    if isinstance(dtype, MapType):
        return MapType(
            _without_metadata(dtype.keyType),
            _without_metadata(dtype.valueType),
            dtype.valueContainsNull,
        )
    return dtype


class _SchemaCheckMixin:
    def _check_schema(self, df: DataFrame, lazy: bool = True) -> list[Violation]:
        violations: list[Violation] = []
        actual: dict[str, DataType] = {f.name: f.dataType for f in df.schema.fields}

        for col_name, field in self._fields.items():
            violation: Violation | None = None

            if col_name not in actual:
                violation = Violation(
                    kind=ViolationKind.MISSING_COLUMN,
                    column=col_name,
                    expected_type=field.dtype.simpleString(),
                )
            elif _without_metadata(actual[col_name]) != _without_metadata(field.dtype):
                expected_type = field.dtype.simpleString()
                actual_type = actual[col_name].simpleString()
                violation = Violation(
                    kind=ViolationKind.TYPE_MISMATCH,
                    column=col_name,
                    expected_type=expected_type,
                    actual_type=actual_type,
                    # The type strings omit nested nullability, so two unequal types can
                    # still render identically.
                    constraint=(
                        "nested nullability differs" if expected_type == actual_type else None
                    ),
                )

            if violation is not None:
                violations.append(violation)
                if not lazy:
                    return violations

        return violations
