import inspect
from collections.abc import Callable
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame

from pyspark_contracts._quality_checks import _RowRule, _severity_for
from pyspark_contracts._report import Severity, Violation, ViolationKind


@dataclass
class _ResolvedCheck:
    """A ``@check`` after calling its method: a condition (``rule``) or failing rows."""

    name: str
    method: Callable
    rule: _RowRule | None = None
    failing: DataFrame | None = None


class _CustomCheckMixin:
    def _route_check_kwargs(self, kwargs: dict, caller: str) -> dict[str, dict]:
        """Each ``@check``'s share of ``kwargs``, by parameter name. Raises ``TypeError`` for a
        kwarg no check accepts, so a typo can't be silently ignored."""
        accepted: set[str] = set()
        routed: dict[str, dict] = {}
        for check_name, method in self._checks.items():
            params = [p for p in inspect.signature(method).parameters if p not in ("self", "df")]
            accepted.update(params)
            routed[check_name] = {k: v for k, v in kwargs.items() if k in params}

        unknown = set(kwargs) - accepted
        if unknown:
            raise TypeError(
                f"{caller} got unexpected keyword argument(s) not accepted by any "
                f"@check: {sorted(unknown)}"
            )
        return routed

    def _resolve_checks(self, df: DataFrame, routed: dict[str, dict]) -> list[_ResolvedCheck]:
        """Calls every ``@check`` method once and sorts the result: a ``Column`` becomes a
        row rule (counted in the single aggregation, usable by ``split()``); a ``DataFrame``
        is kept to be counted on its own."""
        resolved: list[_ResolvedCheck] = []
        for check_name, method in self._checks.items():
            result = method(self, df, **routed[check_name])
            if isinstance(result, Column):
                rule = _RowRule(
                    kind=ViolationKind.CHECK_FAILED,
                    column=None,
                    constraint=method._check_description,
                    fail=result,
                    severity=method._check_severity,
                    max_failure_pct=method._check_max_failure_pct,
                    check=check_name,
                )
                resolved.append(_ResolvedCheck(check_name, method, rule=rule))
            elif isinstance(result, DataFrame):
                resolved.append(_ResolvedCheck(check_name, method, failing=result))
            else:
                raise TypeError(
                    f"@check method '{check_name}' must return a Column condition selecting "
                    f"the failing rows, or a DataFrame of them; got {type(result).__name__}"
                )
        return resolved

    def _check_custom(
        self,
        df: DataFrame,
        row_count: int,
        checks: list[_ResolvedCheck],
        condition_counts: dict[str, int],
        lazy: bool = True,
    ) -> list[Violation]:
        violations: list[Violation] = []
        for resolved in checks:
            method = resolved.method
            if resolved.rule is not None:
                fail = condition_counts[resolved.name]
                failing = df.filter(resolved.rule.fail)
            else:
                failing = resolved.failing
                fail = failing.count()
            if not fail:
                continue

            severity = _severity_for(
                method._check_severity, method._check_max_failure_pct, fail, row_count
            )
            violations.append(
                Violation(
                    kind=ViolationKind.CHECK_FAILED,
                    severity=severity,
                    check=resolved.name,
                    constraint=method._check_description,
                    row_pct=round(fail / row_count * 100, 1),
                    failure_count=fail,
                    sample_values=[row.asDict() for row in failing.limit(5).collect()],
                )
            )
            if not lazy and severity is Severity.ERROR:
                return violations

        return violations
