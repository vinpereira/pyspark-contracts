import dataclasses
from dataclasses import dataclass

from pyspark_contracts._depth import ValidationDepth


@dataclass
class Violation:
    """One failed constraint from a :meth:`.Contract.validate` run.

    Attributes:
        kind: The violation type, e.g. ``"missing_column"``, ``"type_mismatch"``,
            ``"null_violation"``, ``"value_out_of_range"``, ``"length_out_of_range"``,
            ``"regex_mismatch"``, ``"value_not_allowed"``, ``"condition_failed"``,
            ``"check_failed"``, ``"duplicate_value"``, ``"row_count_out_of_range"``.
        column: The field this violation belongs to, or ``None`` for a
            ``row_count_out_of_range`` violation, which is about the DataFrame as a
            whole rather than any single column.
        expected_type: For ``missing_column``/``type_mismatch`` — the declared
            ``Field`` type's class name.
        actual_type: For ``type_mismatch`` — the DataFrame's actual type's class
            name.
        constraint: Human-readable description of the specific constraint that
            failed (e.g. ``"min_value=0"``, ``"nullable"``, a ``condition``'s or
            ``@check``'s description text).
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

    kind: str
    column: str | None = None
    expected_type: str | None = None
    actual_type: str | None = None
    constraint: str | None = None
    row_pct: float | None = None
    failure_count: int | None = None
    sample_values: list | None = None


class ViolationReport:
    """The result of a :meth:`.Contract.validate` call.

    Falsy (and ``is_empty()`` is ``True``) when there are no violations — the common
    ``if report: ...`` / ``if not report: ...`` idiom works as expected.

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
                {k: v for k, v in dataclasses.asdict(violation).items() if v is not None}
                for violation in self.violations
            ],
            "row_count": self.row_count,
        }


class ContractViolationError(Exception):
    """Raised by ``validate(mode="hard")`` (the default) when violations are found.

    Attributes:
        report: The :class:`ViolationReport` describing what failed.
    """

    def __init__(self, report: ViolationReport) -> None:
        self.report = report
        n = len(report.violations)
        super().__init__(f"{report.contract_name} — {n} violation(s) — job aborted")
