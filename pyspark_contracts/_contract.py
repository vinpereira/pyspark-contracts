import logging
import os
from collections.abc import Callable

from pyspark.sql import DataFrame

from pyspark_contracts._custom_checks import _CustomCheckMixin
from pyspark_contracts._depth import ValidationDepth
from pyspark_contracts._documentation import _DocumentationMixin
from pyspark_contracts._field import Field
from pyspark_contracts._logging import get_logger
from pyspark_contracts._quality_checks import _QualityCheckMixin
from pyspark_contracts._report import ContractViolationError, Violation, ViolationReport
from pyspark_contracts._schema_checks import _SchemaCheckMixin


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


class Contract(
    _DocumentationMixin,
    _SchemaCheckMixin,
    _QualityCheckMixin,
    _CustomCheckMixin,
    metaclass=ContractMeta,
):
    _fields: dict[str, Field]
    _checks: dict[str, Callable]

    def validate(
        self,
        df: DataFrame,
        *,
        mode: str = "hard",
        lazy: bool | None = None,
        depth: ValidationDepth = ValidationDepth.SCHEMA_AND_DATA,
        logger: logging.Logger | None = None,
        **kwargs,
    ) -> ViolationReport:
        if os.environ.get("PYSPARK_CONTRACTS_ENABLED", "true").lower() == "false":
            return ViolationReport(type(self).__name__, [], None, mode=mode, depth=depth)

        if lazy is None:
            lazy = mode != "hard"

        _logger = logger or get_logger()
        violations: list[Violation] = []
        row_count: int | None = None
        blocks_cross_column = False

        if depth in (ValidationDepth.SCHEMA_ONLY, ValidationDepth.SCHEMA_AND_DATA):
            violations = self._check_schema(df, lazy=lazy)
            blocks_cross_column = any(
                v.kind in ("missing_column", "type_mismatch") for v in violations
            )

        if depth in (ValidationDepth.DATA_ONLY, ValidationDepth.SCHEMA_AND_DATA) and (
            lazy or not violations
        ):
            row_count = df.count()
            if row_count > 0:
                violated_columns = {v.column for v in violations}
                violations += self._check_quality(
                    df,
                    row_count,
                    skip_columns=violated_columns,
                    lazy=lazy,
                    blocks_cross_column=blocks_cross_column,
                )
            if row_count > 0 and not blocks_cross_column and (lazy or not violations):
                violations += self._check_custom(df, row_count, lazy=lazy, **kwargs)

        report = ViolationReport(type(self).__name__, violations, row_count, mode=mode, depth=depth)

        if violations:
            log_data = {
                "contract": report.contract_name,
                "mode": mode,
                "depth": depth.value,
                "violations": report.to_dict()["violations"],
                "row_count": row_count,
            }
            if mode == "hard":
                _logger.error("contract violation — job aborted", extra=log_data)
                raise ContractViolationError(report)
            else:
                _logger.warning("contract violation — continuing", extra=log_data)

        return report
