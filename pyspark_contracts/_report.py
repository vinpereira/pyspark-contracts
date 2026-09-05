import dataclasses
from dataclasses import dataclass

from pyspark_contracts._depth import ValidationDepth


@dataclass
class Violation:
    kind: str
    column: str
    expected_type: str | None = None
    actual_type: str | None = None
    constraint: str | None = None
    row_pct: float | None = None
    failure_count: int | None = None
    sample_values: list | None = None


class ViolationReport:
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
        return len(self.violations) == 0

    def to_dict(self) -> dict:
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
    def __init__(self, report: ViolationReport) -> None:
        self.report = report
        n = len(report.violations)
        super().__init__(f"{report.contract_name} — {n} violation(s) — job aborted")
