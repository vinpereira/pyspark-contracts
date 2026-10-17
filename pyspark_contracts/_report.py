import dataclasses
from dataclasses import dataclass
from enum import Enum

from pyspark_contracts._depth import ValidationDepth


class _StrEnum(str, Enum):
    # Python 3.11+ formats a (str, Enum) member as "ClassName.X"; keep the plain value on
    # every supported version, as enum.StrEnum (3.11+ only) does.
    def __str__(self) -> str:
        return self.value

    __format__ = str.__format__


class Severity(_StrEnum):
    """How serious a :class:`Violation` is.

    ``ERROR`` violations make ``validate(mode="hard")`` raise; ``WARN`` violations are
    reported and logged but never raise. A ``str`` subclass, like :class:`ViolationKind`:
    ``Severity.WARN == "warn"``.
    """

    ERROR = "error"
    WARN = "warn"


class ViolationKind(_StrEnum):
    """The type of a :class:`Violation`.

    A ``str`` subclass, so each member compares equal to — and formats as — its
    plain string value: ``ViolationKind.NULL_VIOLATION == "null_violation"``.
    """

    MISSING_COLUMN = "missing_column"
    TYPE_MISMATCH = "type_mismatch"
    NULL_VIOLATION = "null_violation"
    VALUE_OUT_OF_RANGE = "value_out_of_range"
    LENGTH_OUT_OF_RANGE = "length_out_of_range"
    REGEX_MISMATCH = "regex_mismatch"
    VALUE_NOT_ALLOWED = "value_not_allowed"
    CONDITION_FAILED = "condition_failed"
    CHECK_FAILED = "check_failed"
    DUPLICATE_VALUE = "duplicate_value"
    ROW_COUNT_OUT_OF_RANGE = "row_count_out_of_range"
    UNEXPECTED_COLUMN = "unexpected_column"


@dataclass
class Violation:
    """One failed constraint from a :meth:`.Contract.validate` run.

    Attributes:
        kind: The violation type, a :class:`ViolationKind`. A plain string such as
            ``"missing_column"`` is accepted and converted; an unknown one raises
            ``ValueError``.
        severity: A :class:`Severity` — ``ERROR`` (the default) or ``WARN``, from the
            rule's ``severity``/``max_failure_pct`` settings. Structural and
            ``row_count_out_of_range`` violations are always ``ERROR``. A plain string
            is accepted and converted.
        column: The field this violation belongs to, or ``None`` for violations
            that aren't about a single column: ``row_count_out_of_range`` (the
            DataFrame as a whole) and ``check_failed`` (see ``check``).
        check: For ``check_failed`` — the name of the ``@check`` method that found
            failing rows.
        expected_type: For ``missing_column``/``type_mismatch`` — the declared
            ``Field`` type as a Spark type string (``DataType.simpleString()``,
            e.g. ``"decimal(26,12)"``, ``"array<string>"``).
        actual_type: For ``type_mismatch``/``unexpected_column`` — the DataFrame's
            actual type, in the same format.
        constraint: Human-readable description of the specific constraint that
            failed (e.g. ``"min_value=0"``, ``"nullable"``, a ``condition``'s or
            ``@check``'s description text). For a ``type_mismatch`` whose two type
            strings are identical, explains that the difference is in nested
            nullability, which type strings don't show.
        row_pct: Percentage of rows that failed this constraint. Not set for
            structural (``missing_column``/``type_mismatch``) or dataset-level
            (``row_count_out_of_range``) violations, which aren't about a subset of
            rows.
        failure_count: Number of rows that failed this constraint. Same
            applicability as ``row_pct``.
        sample_values: Up to 5 offending values, for quick diagnosis. For
            ``check_failed``, these are full rows (as ``dict``) rather than single
            column values, since a ``@check`` can span multiple columns.
    """

    kind: ViolationKind
    severity: Severity = Severity.ERROR
    column: str | None = None
    check: str | None = None
    expected_type: str | None = None
    actual_type: str | None = None
    constraint: str | None = None
    row_pct: float | None = None
    failure_count: int | None = None
    sample_values: list | None = None

    def __post_init__(self) -> None:
        self.kind = ViolationKind(self.kind)
        self.severity = Severity(self.severity)


class ViolationReport:
    """The result of a :meth:`.Contract.validate` call.

    Falsy (and ``is_empty()`` is ``True``) when there are no violations — the common
    ``if report: ...`` / ``if not report: ...`` idiom works as expected. Warnings count as
    violations here; use :attr:`errors`/:attr:`warnings` to tell them apart.

    Attributes:
        contract_name: Name of the contract that was validated (the ``Contract``
            subclass's name, or the original contract's name when validated via a
            reloaded :class:`.ContractSchema`).
        violations: The :class:`Violation` list found — empty if validation passed.
        row_count: The DataFrame's row count, or ``None`` when it was never computed
            (``PYSPARK_CONTRACTS_ENABLED=false``, or ``depth=ValidationDepth.SCHEMA_ONLY``).
        mode: The ``mode`` the validation ran with (``"hard"`` or ``"soft"``).
        depth: The :class:`.ValidationDepth` the validation ran with.
    """

    def __init__(
        self,
        contract_name: str,
        violations: list[Violation],
        row_count: int | None,
        mode: str = "hard",
        depth: ValidationDepth = ValidationDepth.SCHEMA_AND_DATA,
    ) -> None:
        self.contract_name = contract_name
        self.violations = violations
        self.row_count = row_count
        self.mode = mode
        self.depth = depth

    def __bool__(self) -> bool:
        return len(self.violations) > 0

    def is_empty(self) -> bool:
        """Whether validation found no violations. Equivalent to ``not report``."""
        return len(self.violations) == 0

    @property
    def errors(self) -> list[Violation]:
        """The ``ERROR``-severity violations — the ones that make ``mode="hard"`` raise."""
        return [v for v in self.violations if v.severity is Severity.ERROR]

    @property
    def warnings(self) -> list[Violation]:
        """The ``WARN``-severity violations — reported and logged, never raised."""
        return [v for v in self.violations if v.severity is Severity.WARN]

    def to_dict(self) -> dict:
        """Serializes this report to a plain ``dict`` — the same shape logged as
        JSON on a violation, with each :class:`Violation`'s unset (``None``) fields
        omitted.
        """
        return {
            "contract": self.contract_name,
            "mode": self.mode,
            "depth": self.depth.value,
            "violations": [
                {
                    k: v.value if isinstance(v, _StrEnum) else v
                    for k, v in dataclasses.asdict(violation).items()
                    if v is not None
                }
                for violation in self.violations
            ],
            "row_count": self.row_count,
        }


class ContractViolationError(Exception):
    """Raised by ``validate(mode="hard")`` (the default) when at least one
    ``ERROR``-severity violation is found. Warnings alone never raise it.

    Attributes:
        report: The :class:`ViolationReport` describing what failed.
    """

    def __init__(self, report: ViolationReport) -> None:
        self.report = report
        if report.warnings:
            counts = f"{len(report.errors)} error(s), {len(report.warnings)} warning(s)"
        else:
            counts = f"{len(report.violations)} violation(s)"
        super().__init__(f"{report.contract_name} — {counts} — job aborted")
