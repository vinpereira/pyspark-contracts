from collections.abc import Callable

from pyspark_contracts._field import _check_failure_pct, _severity


def check(
    description: str, *, severity: str = "error", max_failure_pct: float | None = None
) -> Callable[[Callable], Callable]:
    """Marks a :class:`.Contract` method as a cross-column data quality check.

    The decorated method is collected automatically by ``Contract``'s metaclass — it
    doesn't need to be called directly. It must accept ``self`` and ``df`` (the full
    DataFrame being validated) and describe the **failing rows**, in one of two ways:

    - **A** ``Column`` **condition** selecting them — what you'd pass to ``df.filter(...)``.
      It's evaluated in the same single scan as the field constraints, so a passing check
      costs no extra Spark action, and :meth:`.Contract.split` can reject rows with it.
      Prefer this whenever the rule is a per-row condition.
    - **A** ``DataFrame`` of the failing rows, for rules that need joins, windows, or
      aggregations. Costs one Spark action, and ``split()`` can't attribute it to rows.

    A row where the condition is null doesn't fail, as with ``filter``. Note the polarity is
    the opposite of ``Field(condition=...)``, which describes the rows that *pass*.

    The method is called once per validation that reaches the data stage. Extra parameters
    are supplied via ``validate(df, **kwargs)`` (or ``split(df, **kwargs)``) and routed only
    to the checks that declare them.

    Skipped entirely when the DataFrame has any ``missing_column``/``type_mismatch``
    violation, since the method may reference arbitrary columns.

    Args:
        description: Human-readable text describing what the check verifies (e.g.
            ``"delta_km must be non-negative"``). Used as the resulting
            ``check_failed`` violation's ``constraint`` text, and exported by
            ``to_dict()``/``describe()`` — the check's code itself is never
            serialized.
        severity: ``"error"`` (the default) or ``"warn"``. A ``"warn"`` check's
            failures are reported and logged but never make ``mode="hard"`` raise.
        max_failure_pct: A tolerance, as a percentage of rows (0–100): a check failing on
            at most this share of rows is reported as a warning instead of an error.
            Ignored by ``split()``.

    Example:
        >>> class FinalDfContract(Contract):
        ...     @check("delta_km must be non-negative")
        ...     def no_regression(self, df):
        ...         return F.col("delta_km") < 0
    """

    check_severity = _severity(severity, "check()")
    _check_failure_pct(max_failure_pct, "check()")

    def decorator(func: Callable) -> Callable:
        func._check_description = description
        func._check_severity = check_severity
        func._check_max_failure_pct = max_failure_pct
        return func

    return decorator
