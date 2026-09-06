import logging
import os

from pyspark.sql import DataFrame

from pyspark_contracts._depth import ValidationDepth
from pyspark_contracts._logging import get_logger
from pyspark_contracts._report import ContractViolationError, Violation, ViolationReport


class _ValidationMixin:
    def _report_name(self) -> str:
        return type(self).__name__

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
            return ViolationReport(self._report_name(), [], None, mode=mode, depth=depth)

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

        report = ViolationReport(self._report_name(), violations, row_count, mode=mode, depth=depth)

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
