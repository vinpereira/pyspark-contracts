import inspect

from pyspark.sql import DataFrame

from pyspark_contracts._report import Violation


class _CustomCheckMixin:
    def _check_custom(
        self, df: DataFrame, row_count: int, lazy: bool = True, **kwargs
    ) -> list[Violation]:
        accepted: set[str] = set()
        check_kwargs: dict[str, dict] = {}
        for check_name, method in self._checks.items():
            params = [p for p in inspect.signature(method).parameters if p not in ("self", "df")]
            accepted.update(params)
            check_kwargs[check_name] = {k: v for k, v in kwargs.items() if k in params}

        unknown = set(kwargs) - accepted
        if unknown:
            raise TypeError(
                f"validate() got unexpected keyword argument(s) not accepted by any "
                f"@check: {sorted(unknown)}"
            )

        violations: list[Violation] = []
        for check_name, method in self._checks.items():
            failing_df = method(self, df, **check_kwargs[check_name])
            fail = failing_df.count()
            if fail:
                violations.append(
                    Violation(
                        kind="check_failed",
                        column=check_name,
                        constraint=method._check_description,
                        row_pct=round(fail / row_count * 100, 1),
                        failure_count=fail,
                        sample_values=[row.asDict() for row in failing_df.limit(5).collect()],
                    )
                )
                if not lazy:
                    return violations

        return violations
