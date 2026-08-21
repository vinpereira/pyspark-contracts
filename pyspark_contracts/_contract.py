import logging

from pyspark.sql import DataFrame
from pyspark.sql.types import DataType

from pyspark_contracts._field import Field
from pyspark_contracts._logging import get_logger
from pyspark_contracts._report import ContractViolationError, Violation, ViolationReport


class ContractMeta(type):
    def __new__(mcs, name, bases, namespace):
        fields: dict[str, Field] = {}
        for attr_name, value in namespace.items():
            if isinstance(value, Field):
                fields[attr_name] = value
        namespace["_fields"] = fields
        return super().__new__(mcs, name, bases, namespace)


class Contract(metaclass=ContractMeta):
    _fields: dict[str, Field]

    def validate(
        self,
        df: DataFrame,
        *,
        mode: str = "hard",
        logger: logging.Logger | None = None,
    ) -> ViolationReport:
        _logger = logger or get_logger()
        row_count = df.count()
        violations = self._check_schema(df, row_count)
        report = ViolationReport(type(self).__name__, violations, row_count, mode=mode)

        if violations:
            log_data = {
                "contract": report.contract_name,
                "mode": mode,
                "violations": report.to_dict()["violations"],
                "row_count": row_count,
            }
            if mode == "hard":
                _logger.error("contract violation — job aborted", extra=log_data)
                raise ContractViolationError(report)
            else:
                _logger.warning("contract violation — continuing", extra=log_data)

        return report

    def _check_schema(self, df: DataFrame, row_count: int) -> list[Violation]:
        violations: list[Violation] = []
        actual: dict[str, DataType] = {f.name: f.dataType for f in df.schema.fields}

        for col_name, field in self._fields.items():
            if col_name not in actual:
                violations.append(
                    Violation(
                        kind="missing_column",
                        column=col_name,
                        expected_type=type(field.dtype).__name__,
                    )
                )
            elif actual[col_name] != field.dtype:
                violations.append(
                    Violation(
                        kind="type_mismatch",
                        column=col_name,
                        expected_type=type(field.dtype).__name__,
                        actual_type=type(actual[col_name]).__name__,
                    )
                )
            elif not field.nullable and row_count > 0:
                from pyspark.sql import functions as F

                null_count = df.filter(F.col(col_name).isNull()).count()
                if null_count > 0:
                    violations.append(
                        Violation(
                            kind="null_violation",
                            column=col_name,
                            row_pct=round(null_count / row_count * 100, 1),
                        )
                    )

        return violations

    def _check_quality(
        self, df: DataFrame, row_count: int, skip_columns: set[str]
    ) -> list[Violation]:
        return []  # implemented in Task 8
