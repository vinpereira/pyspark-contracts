import os
import warnings
from functools import reduce
from operator import and_
from typing import NamedTuple

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from pyspark_contracts._depth import ValidationDepth
from pyspark_contracts._quality_checks import _quote, _RowRule
from pyspark_contracts._report import ContractViolationError, Severity, ViolationReport

_VIOLATION_TYPE = StructType(
    [
        StructField("kind", StringType()),
        StructField("column", StringType()),
        StructField("check", StringType()),
        StructField("constraint", StringType()),
    ]
)


class SplitResult(NamedTuple):
    """What :meth:`.Contract.split` returns: ``valid, rejected = contract.split(df)``.

    Attributes:
        valid: The rows that broke no error-level row rule, with the original columns.
        rejected: The rows that broke at least one, with the original columns plus an
            array column listing the rules each row broke.
    """

    valid: DataFrame
    rejected: DataFrame


def _is_duplicate(rule: _RowRule) -> Column:
    """Whether a row belongs to a group sharing its (non-null) values in the rule's columns."""
    quoted = [F.col(_quote(c)) for c in (rule.columns or (rule.column,))]
    share_values = F.count(F.lit(1)).over(Window.partitionBy(*quoted)) > 1
    return share_values & reduce(and_, [c.isNotNull() for c in quoted])


def _violation(rule: _RowRule) -> Column:
    return F.struct(
        F.lit(rule.kind.value).alias("kind"),
        F.lit(rule.column).cast("string").alias("column"),
        F.lit(rule.check).cast("string").alias("check"),
        F.lit(rule.constraint).alias("constraint"),
    )


class _SplitMixin:
    def split(self, df: DataFrame, *, errors_column: str = "_violations", **kwargs) -> SplitResult:
        """Separates the rows that pass this contract's row-level rules from those that don't,
        so a job can quarantine the bad ones and carry on with the rest.

        Returns ``valid, rejected``: ``valid`` has the original columns; ``rejected`` adds
        ``errors_column``, an ``array<struct<kind, column, check, constraint>>`` of every rule
        the row broke, in declaration order. ``column`` is null for ``unique_together`` and
        checks; ``check`` holds a check's name and is null for everything else.
        Every row lands in exactly one of the two.

        Lazy: it runs no Spark action and logs nothing, and ``valid`` and ``rejected`` are two
        plans over ``df`` — cache ``df`` if reading it is expensive.

        Which rules apply:

        - The row-level ones: field constraints (including ``blank_is_null``), ``unique``,
          ``unique_together``, and ``@check`` methods that return a condition. A rule whose
          predicate is null for a row (e.g. ``min_value`` on a null) doesn't reject it,
          exactly as in :meth:`validate`.
        - Only ``error``-severity rules reject rows; a ``severity="warn"`` rule never takes a
          row out of ``valid``.
        - ``max_failure_pct`` is ignored: it's a judgment about the whole DataFrame and would
          need a count. Use :meth:`validate` for tolerances.
        - ``@check`` methods that return a DataFrame can't be attributed to input rows; they
          are not evaluated and a ``UserWarning`` names them.
        - ``min_rows``/``max_rows`` are dataset-level and ignored.

        With ``PYSPARK_CONTRACTS_ENABLED=false`` every row is valid and ``rejected`` is empty.

        Args:
            df: The DataFrame to split.
            errors_column: Name of the column added to ``rejected``. Defaults to
                ``"_violations"``.
            **kwargs: Forwarded to the ``@check`` methods, as in :meth:`validate`; a kwarg no
                check accepts raises ``TypeError``.

        Returns:
            A :class:`.SplitResult`, unpackable as ``valid, rejected``.

        Raises:
            ContractViolationError: If ``df`` has a structural problem (missing, mistyped, or —
                for a ``strict`` contract — unexpected columns), which makes row rules
                meaningless.
            ValueError: If ``df`` already has a column named ``errors_column``.
        """
        if errors_column in df.columns:
            raise ValueError(f"split() errors_column {errors_column!r} already exists in the df")

        enabled = os.environ.get("PYSPARK_CONTRACTS_ENABLED", "true").lower() != "false"
        rules: list[_RowRule] = []
        if enabled:
            structural = self._check_schema(df, lazy=True)
            if structural:
                report = ViolationReport(
                    self._report_name(),
                    structural,
                    None,
                    mode="hard",
                    depth=ValidationDepth.SCHEMA_ONLY,
                )
                raise ContractViolationError(report)
            checks = self._resolve_checks(df, self._route_check_kwargs(kwargs, "split()"))
            rules = [
                rule
                for rule in self._row_rules(skip_columns=set(), blocks_cross_column=False)
                + [c.rule for c in checks if c.rule is not None]
                if rule.severity is Severity.ERROR
            ]
            dataframe_checks = sorted(c.name for c in checks if c.rule is None)
            if dataframe_checks:
                warnings.warn(
                    f"split() doesn't evaluate @check methods that return a DataFrame (their "
                    f"result can't be attributed to input rows): {dataframe_checks}",
                    stacklevel=2,
                )

        broken = [
            F.when(_is_duplicate(rule) if rule.fail is None else rule.fail, _violation(rule))
            for rule in rules
        ]
        # A typed null first keeps the array's element type stable when there are no rules
        no_violation = F.lit(None).cast(_VIOLATION_TYPE)
        tagged = df.withColumn(
            errors_column,
            F.filter(F.array(no_violation, *broken), lambda v: v.isNotNull()),
        )
        valid = tagged.filter(F.size(errors_column) == 0).drop(errors_column)
        rejected = tagged.filter(F.size(errors_column) > 0)
        return SplitResult(valid, rejected)
