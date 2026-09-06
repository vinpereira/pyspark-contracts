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
                    expected_type=type(field.dtype).__name__,
                )
            elif actual[col_name] != field.dtype:
                violation = Violation(
                    kind="type_mismatch",
                    column=col_name,
                    expected_type=type(field.dtype).__name__,
                    actual_type=type(actual[col_name]).__name__,
                )

            if violation is not None:
                violations.append(violation)
                if not lazy:
                    return violations

        return violations
