import inspect
import logging
import os
from collections.abc import Callable

from pyspark.sql import DataFrame
from pyspark.sql.types import DataType

from pyspark_contracts._field import Field
from pyspark_contracts._logging import get_logger
from pyspark_contracts._report import ContractViolationError, Violation, ViolationReport


class ContractMeta(type):
    def __new__(mcs, name, bases, namespace):
        fields: dict[str, Field] = {}
        checks: dict[str, Callable] = {}
        for attr_name, value in namespace.items():
            if isinstance(value, Field):
                fields[attr_name] = value
            elif callable(value) and hasattr(value, "_check_description"):
                checks[attr_name] = value
        namespace["_fields"] = fields
        namespace["_checks"] = checks
        return super().__new__(mcs, name, bases, namespace)


class Contract(metaclass=ContractMeta):
    _fields: dict[str, Field]
    _checks: dict[str, Callable]

    def validate(
        self,
        df: DataFrame,
        *,
        mode: str = "hard",
        lazy: bool | None = None,
        logger: logging.Logger | None = None,
        **kwargs,
    ) -> ViolationReport:
        if os.environ.get("PYSPARK_CONTRACTS_ENABLED", "true").lower() == "false":
            return ViolationReport(type(self).__name__, [], None, mode=mode)

        if lazy is None:
            lazy = mode != "hard"

        _logger = logger or get_logger()
        row_count = df.count()
        violations = self._check_schema(df, row_count, lazy=lazy)
        if row_count > 0 and (lazy or not violations):
            violated_columns = {v.column for v in violations}
            violations += self._check_quality(
                df, row_count, skip_columns=violated_columns, lazy=lazy
            )
        if row_count > 0 and (lazy or not violations):
            violations += self._check_custom(df, row_count, lazy=lazy, **kwargs)
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

    def _check_schema(self, df: DataFrame, row_count: int, lazy: bool = True) -> list[Violation]:
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
            elif not field.nullable and row_count > 0:
                from pyspark.sql import functions as F

                condition = F.col(col_name).isNull()
                null_count = df.filter(condition).count()
                if null_count > 0:
                    violation = Violation(
                        kind="null_violation",
                        column=col_name,
                        constraint="nullable",
                        row_pct=round(null_count / row_count * 100, 1),
                        failure_count=null_count,
                        sample_values=self._sample_values(df, col_name, condition),
                    )

            if violation is not None:
                violations.append(violation)
                if not lazy:
                    return violations

        return violations

    def _sample_values(self, df: DataFrame, col_name: str, condition) -> list:
        rows = df.filter(condition).select(col_name).limit(5).collect()
        return [row[0] for row in rows]

    def _check_quality(
        self, df: DataFrame, row_count: int, skip_columns: set[str], lazy: bool = True
    ) -> list[Violation]:
        from pyspark.sql import functions as F

        violations: list[Violation] = []

        for col_name, field in self._fields.items():
            if col_name in skip_columns or not field.has_quality_constraints():
                continue

            if field.min_value is not None:
                condition = F.col(col_name) < field.min_value
                fail = df.filter(condition).count()
                if fail:
                    violations.append(
                        Violation(
                            kind="value_out_of_range",
                            column=col_name,
                            constraint=f"min_value={field.min_value}",
                            row_pct=round(fail / row_count * 100, 1),
                            failure_count=fail,
                            sample_values=self._sample_values(df, col_name, condition),
                        )
                    )
                    if not lazy:
                        return violations

            if field.max_value is not None:
                condition = F.col(col_name) > field.max_value
                fail = df.filter(condition).count()
                if fail:
                    violations.append(
                        Violation(
                            kind="value_out_of_range",
                            column=col_name,
                            constraint=f"max_value={field.max_value}",
                            row_pct=round(fail / row_count * 100, 1),
                            failure_count=fail,
                            sample_values=self._sample_values(df, col_name, condition),
                        )
                    )
                    if not lazy:
                        return violations

            if field.min_length is not None:
                condition = F.length(F.col(col_name)) < field.min_length
                fail = df.filter(condition).count()
                if fail:
                    violations.append(
                        Violation(
                            kind="length_out_of_range",
                            column=col_name,
                            constraint=f"min_length={field.min_length}",
                            row_pct=round(fail / row_count * 100, 1),
                            failure_count=fail,
                            sample_values=self._sample_values(df, col_name, condition),
                        )
                    )
                    if not lazy:
                        return violations

            if field.max_length is not None:
                condition = F.length(F.col(col_name)) > field.max_length
                fail = df.filter(condition).count()
                if fail:
                    violations.append(
                        Violation(
                            kind="length_out_of_range",
                            column=col_name,
                            constraint=f"max_length={field.max_length}",
                            row_pct=round(fail / row_count * 100, 1),
                            failure_count=fail,
                            sample_values=self._sample_values(df, col_name, condition),
                        )
                    )
                    if not lazy:
                        return violations

            if field.regex is not None:
                condition = ~F.col(col_name).rlike(field.regex)
                fail = df.filter(condition).count()
                if fail:
                    violations.append(
                        Violation(
                            kind="regex_mismatch",
                            column=col_name,
                            constraint=f"regex={field.regex!r}",
                            row_pct=round(fail / row_count * 100, 1),
                            failure_count=fail,
                            sample_values=self._sample_values(df, col_name, condition),
                        )
                    )
                    if not lazy:
                        return violations

            if field.allowed_values is not None:
                condition = ~F.col(col_name).isin(field.allowed_values)
                fail = df.filter(condition).count()
                if fail:
                    violations.append(
                        Violation(
                            kind="value_not_allowed",
                            column=col_name,
                            constraint=f"allowed_values={field.allowed_values}",
                            row_pct=round(fail / row_count * 100, 1),
                            failure_count=fail,
                            sample_values=self._sample_values(df, col_name, condition),
                        )
                    )
                    if not lazy:
                        return violations

            if field.condition is not None:
                condition = ~field.condition(col_name)
                fail = df.filter(condition).count()
                if fail:
                    violations.append(
                        Violation(
                            kind="condition_failed",
                            column=col_name,
                            constraint=field.condition_description or "condition",
                            row_pct=round(fail / row_count * 100, 1),
                            failure_count=fail,
                            sample_values=self._sample_values(df, col_name, condition),
                        )
                    )
                    if not lazy:
                        return violations

        return violations

    def _check_custom(
        self, df: DataFrame, row_count: int, lazy: bool = True, **kwargs
    ) -> list[Violation]:
        accepted: set[str] = set()
        check_kwargs: dict[str, dict] = {}
        for check_name, method in self._checks.items():
            params = [p for p in inspect.signature(method).parameters if p not in ("self", "df")]
            accepted.update(params)
            check_kwargs[check_name] = {k: v for k, v in kwargs.items() if k in params}

        unknown = set(kwargs) - accepted
        if unknown:
            raise TypeError(
                f"validate() got unexpected keyword argument(s) not accepted by any "
                f"@check: {sorted(unknown)}"
            )

        violations: list[Violation] = []
        for check_name, method in self._checks.items():
            failing_df = method(self, df, **check_kwargs[check_name])
            fail = failing_df.count()
            if fail:
                violations.append(
                    Violation(
                        kind="check_failed",
                        column=check_name,
                        constraint=method._check_description,
                        row_pct=round(fail / row_count * 100, 1),
                        failure_count=fail,
                        sample_values=[row.asDict() for row in failing_df.limit(5).collect()],
                    )
                )
                if not lazy:
                    return violations

        return violations
