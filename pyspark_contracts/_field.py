from collections.abc import Callable
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import Column
from pyspark.sql.types import DataType

_Comparable = int | float | Decimal | date | datetime


class Field:
    """Declares one column's expected type and constraints on a :class:`.Contract`.

    A ``Field`` combines a structural requirement (the column's Spark type and
    whether it may contain nulls) with optional data-quality constraints. Only the
    constraints you actually set are enforced — a bare ``Field(StringType())`` checks
    just column presence, type, and nothing else.

    Args:
        dtype: The expected Spark ``DataType`` for this column (e.g. ``StringType()``,
            ``DecimalType(26, 12)``). Compared against the DataFrame's actual schema;
            a mismatch produces a ``type_mismatch`` violation.
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
        regex: A pattern the column's string values must fully match (via
            ``rlike``). Non-matching values produce ``regex_mismatch``.
        allowed_values: An allowlist of valid values. Anything else produces
            ``value_not_allowed``.
        condition: A cross-column check anchored to this field. Receives the column
            name and returns a Spark ``Column`` expression that must be ``True`` for
            the row to pass — typically comparing this column against another, e.g.
            ``lambda f: F.col(f) < F.col("end_dt")``. Rows where it evaluates false
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
            than the other constraints (needs a ``groupBy``, not a plain
            ``filter().count()``); null values are never considered duplicates of
            each other.
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
    ) -> None:
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
