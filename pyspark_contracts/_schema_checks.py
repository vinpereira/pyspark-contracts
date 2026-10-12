from pyspark.sql import DataFrame
from pyspark.sql.types import DataType

from pyspark_contracts._report import Violation


class _SchemaCheckMixin:
    def _check_schema(self, df: DataFrame, lazy: bool = True) -> list[Violation]:
        violations: list[Violation] = []
        actual: dict[str, DataType] = {f.name: f.dataType for f in df.schema.fields}

        for col_name, field in self._fields.items():
            violation: Violation | None = None

            if col_name not in actual:
                violation = Violation(
                    kind="missing_column",
                    column=col_name,
                    expected_type=field.dtype.simpleString(),
                )
            elif actual[col_name] != field.dtype:
                expected_type = field.dtype.simpleString()
                actual_type = actual[col_name].simpleString()
                violation = Violation(
                    kind="type_mismatch",
                    column=col_name,
                    expected_type=expected_type,
                    actual_type=actual_type,
                    # The type strings omit nested nullability and field metadata, so
                    # two unequal types can still render identically.
                    constraint=(
                        "nested nullability or field metadata differs"
                        if expected_type == actual_type
                        else None
                    ),
                )

            if violation is not None:
                violations.append(violation)
                if not lazy:
                    return violations

        return violations
