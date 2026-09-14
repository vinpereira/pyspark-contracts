import logging
import os

from pyspark.sql import DataFrame

from pyspark_contracts._depth import ValidationDepth
from pyspark_contracts._logging import get_logger
from pyspark_contracts._report import ContractViolationError, Violation, ViolationReport


class _ValidationMixin:
    min_rows: int | None = None
    max_rows: int | None = None

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
        """Validates ``df`` against this contract's declared fields and checks.

        Runs, in order: structural checks (column presence/type), the dataset-level
        ``min_rows``/``max_rows`` check, per-field data constraints, and ``@check``
        methods — each stage gated by ``depth`` and, in fail-fast mode, by whether an
        earlier stage already found a violation.

        Args:
            df: The DataFrame to validate.
            mode: ``"hard"`` (default) logs at ``ERROR`` and raises
                ``ContractViolationError`` on any violation. ``"soft"`` logs at
                ``WARNING`` and returns the ``ViolationReport`` instead.
            lazy: Whether to collect every violation (``True``) or stop at the first
                one found (``False``), across every stage. Defaults to ``None``,
                which resolves to fail-fast for ``mode="hard"`` and collect-all for
                ``mode="soft"``; pass explicitly to override either default.
            depth: Which stage(s) to run — see :class:`.ValidationDepth`. Defaults to
                ``ValidationDepth.SCHEMA_AND_DATA`` (everything).
            logger: A ``logging.Logger`` to emit the structured JSON log entry to.
                Defaults to this library's built-in logger (JSON to stdout).
            **kwargs: Forwarded to this contract's ``@check`` methods, routed by
                parameter name — a kwarg matching no ``@check``'s signature raises
                ``TypeError``.

        Returns:
            The :class:`.ViolationReport`. In ``mode="hard"``, only returned when
            there were no violations — otherwise ``ContractViolationError`` is
            raised instead.

        Raises:
            ContractViolationError: In ``mode="hard"``, if any violation was found.
            ValueError: If ``mode`` isn't ``"hard"`` or ``"soft"``.
            TypeError: If ``depth`` isn't a :class:`.ValidationDepth`, or if a
                ``**kwargs`` entry doesn't match any ``@check``'s parameters.
        """
        if mode not in ("hard", "soft"):
            raise ValueError(f"validate() mode must be 'hard' or 'soft', got {mode!r}")
        if not isinstance(depth, ValidationDepth):
            raise TypeError(f"validate() depth must be a ValidationDepth, got {depth!r}")

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
            violations += self._check_dataset(row_count, lazy=lazy)
            if row_count > 0 and (lazy or not violations):
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
