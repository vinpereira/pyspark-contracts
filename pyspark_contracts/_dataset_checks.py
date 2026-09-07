from pyspark_contracts._report import Violation


class _DatasetCheckMixin:
    def _check_dataset(self, row_count: int, lazy: bool = True) -> list[Violation]:
        violations: list[Violation] = []

        if self.min_rows is not None and row_count < self.min_rows:
            violations.append(
                Violation(
                    kind="row_count_out_of_range",
                    column=None,
                    constraint=f"min_rows={self.min_rows} (actual: {row_count})",
                )
            )
            if not lazy:
                return violations

        if self.max_rows is not None and row_count > self.max_rows:
            violations.append(
                Violation(
                    kind="row_count_out_of_range",
                    column=None,
                    constraint=f"max_rows={self.max_rows} (actual: {row_count})",
                )
            )
            if not lazy:
                return violations

        return violations
