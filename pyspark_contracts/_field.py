from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import Column
from pyspark.sql.types import DataType, StringType

from pyspark_contracts._report import Severity

_Comparable = int | float | Decimal | date | datetime


def _severity(value: str, caller: str) -> Severity:
    try:
        return Severity(value)
    except ValueError:
        raise ValueError(f"{caller} severity must be 'error' or 'warn', got {value!r}") from None


class Field:
    """Declares one column's expected type and constraints on a :class:`.Contract`.

    A ``Field`` combines a structural requirement (the column's Spark type and
    whether it may contain nulls) with optional data-quality constraints. Only the
    constraints you actually set are enforced — a bare ``Field(StringType())`` checks
    just column presence, type, and nothing else.

    Args:
        dtype: The expected Spark ``DataType`` for this column (e.g. ``StringType()``,
            ``DecimalType(26, 12)``). Compared against the DataFrame's actual schema;
            a mismatch produces a ``type_mismatch`` violation. The metadata of fields
            nested inside a ``StructType`` (comments, units, ...) is ignored, since it's
            documentation rather than part of the data's shape.
        nullable: Whether the column may contain null values. Defaults to ``True``.
            Set ``False`` to produce a ``null_violation`` when any row is null.
        min_value: Minimum allowed value (numeric or date/timestamp types). Values
            below it produce ``value_out_of_range``.
        max_value: Maximum allowed value. Values above it produce
            ``value_out_of_range``.
        min_length: Minimum string length. Shorter values produce
            ``length_out_of_range``.
        max_length: Maximum string length. Longer values produce
            ``length_out_of_range``.
        regex: A pattern each string value must match in full — it's anchored for
            you, so ``"[0-9]+"`` rejects ``"abc123"`` (no ``^``/``$`` needed). Uses
            Java regex syntax, as Spark's ``rlike`` does. Non-matching values produce
            ``regex_mismatch``.
        allowed_values: An allowlist of valid values. Anything else produces
            ``value_not_allowed``.
        condition: A cross-column check anchored to this field. Receives the column
            name and returns a Spark ``Column`` expression that must be ``True`` for
            the row to pass — typically comparing this column against another, e.g.
            ``lambda f: F.col(f) < F.col("end_dt")``. The name is passed as declared;
            if it contains dots or backticks, quote it yourself:
            ``F.col(f"`{f}`")``. Rows where it evaluates false
            produce ``condition_failed``. Skipped entirely when the DataFrame has any
            ``missing_column``/``type_mismatch`` violation, since it may reference
            columns that don't exist.
        condition_description: Human-readable text describing what ``condition``
            checks (e.g. ``"start_dt must precede end_dt"``). Used as the violation's
            ``constraint`` text, and exported by ``to_dict()``/``describe()`` — the
            ``condition`` callable itself is never serialized.
        description: Free-text documentation for this field. Never affects
            validation; surfaced by ``to_dict()``/``to_json()``/``describe()``.
        metadata: A dict of arbitrary documentation (e.g. ``{"unit": "km"}``). Never
            affects validation; surfaced by ``to_dict()``/``describe()``.
        unique: Whether this column's non-null values must all be distinct. Set
            ``True`` to produce ``duplicate_value`` when any value repeats. Costlier
            than the other constraints: it needs a ``groupBy``, so it runs as its own
            Spark action instead of sharing the single pass that checks the others.
            Null values are never considered duplicates of each other.
        severity: ``"error"`` (the default) or ``"warn"``. Applies to every data
            constraint on this field (not to ``missing_column``/``type_mismatch``,
            which are always errors). A ``"warn"`` violation is reported and logged
            but never makes ``mode="hard"`` raise.
        max_failure_pct: A tolerance, as a percentage of rows (0–100, like
            ``row_pct``). A data constraint failing on at most this share of rows is
            reported as a ``"warn"`` violation instead of an error, so it stays
            visible without aborting the job. Compared against the exact share, not
            the rounded ``row_pct``.
        column: The DataFrame column this field stands for, when it differs from the
            attribute name — typically a name that isn't a valid Python identifier, like
            ``"accumulatedData.duration"``. Reports, ``to_dict()``, and the other
            settings of the contract (``key``, ``unique_together``) all use this real
            column name. Two attributes can't declare the same column, but a subclass
            can override an inherited field by declaring its column again.
        blank_is_null: ``True`` makes the ``nullable=False`` check also fail for strings
            that are empty or whitespace-only (any Unicode whitespace), reported as a
            ``null_violation``. Only affects that check — a blank string still counts
            for ``min_length``, ``regex``, and the rest. Requires ``nullable=False`` and a
            ``StringType``, so it can't silently do nothing.
    """

    def __init__(
        self,
        dtype: DataType,
        *,
        nullable: bool = True,
        min_value: _Comparable | None = None,
        max_value: _Comparable | None = None,
        min_length: int | None = None,
        max_length: int | None = None,
        regex: str | None = None,
        allowed_values: list | None = None,
        condition: Callable[[str], Column] | None = None,
        condition_description: str | None = None,
        description: str | None = None,
        metadata: dict | None = None,
        unique: bool = False,
        severity: str = "error",
        max_failure_pct: float | None = None,
        column: str | None = None,
        blank_is_null: bool = False,
    ) -> None:
        if max_failure_pct is not None and not 0 <= max_failure_pct <= 100:
            raise ValueError(
                f"Field() max_failure_pct must be a percentage between 0 and 100, "
                f"got {max_failure_pct!r}"
            )
        if column is not None and not column:
            raise ValueError("Field() column must not be empty")
        if blank_is_null:
            if nullable:
                raise ValueError(
                    "Field() blank_is_null only applies to required fields; set nullable=False"
                )
            if not isinstance(dtype, StringType):
                raise ValueError(
                    f"Field() blank_is_null only applies to StringType fields, got {dtype!r}"
                )
        self.dtype = dtype
        self.nullable = nullable
        self.min_value = min_value
        self.max_value = max_value
        self.min_length = min_length
        self.max_length = max_length
        self.regex = regex
        self.allowed_values = allowed_values
        self.condition = condition
        self.condition_description = condition_description
        self.description = description
        self.metadata = metadata
        self.unique = unique
        self.severity = _severity(severity, "Field()")
        self.max_failure_pct = max_failure_pct
        self.column = column
        self.blank_is_null = blank_is_null

    def _has_quality_constraints(self) -> bool:
        """Whether this field declares at least one row-level data constraint.

        True if ``nullable=False``, ``unique=True``, ``condition`` is set, or any of
        ``min_value``/``max_value``/``min_length``/``max_length``/``regex``/
        ``allowed_values`` is set. Fields declaring only the structural ``dtype`` (or
        only documentation via ``description``/``metadata``) return ``False``.
        """
        return any(
            [
                not self.nullable,
                self.min_value is not None,
                self.max_value is not None,
                self.min_length is not None,
                self.max_length is not None,
                self.regex is not None,
                self.allowed_values is not None,
                self.condition is not None,
                self.unique,
            ]
        )
