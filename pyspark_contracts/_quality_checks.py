from dataclasses import dataclass
from functools import reduce
from operator import and_

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from pyspark_contracts._report import Severity, Violation, ViolationKind


@dataclass
class _RowRule:
    """One row-level constraint on one column — or, for ``unique_together``, on several.

    ``fail`` selects the rows that break it. ``unique`` and ``unique_together`` rules have no
    ``fail``: they need a ``groupBy``, so they can't be part of the single aggregation and are
    counted on demand. ``column`` is ``None`` for ``unique_together``, whose ``columns`` hold
    the group.
    """

    kind: ViolationKind
    column: str | None
    constraint: str
    fail: Column | None = None
    severity: Severity = Severity.ERROR
    max_failure_pct: float | None = None
    columns: tuple[str, ...] = ()

    def severity_for(self, fail: int, row_count: int) -> Severity:
        """This rule's severity for ``fail`` failing rows out of ``row_count``: a warning
        when within ``max_failure_pct`` (compared unrounded), else its own severity."""
        if self.max_failure_pct is not None and fail / row_count * 100 <= self.max_failure_pct:
            return Severity.WARN
        return self.severity


# A string that is empty or whitespace-only. (?U) makes \s Unicode-aware: Spark's trim()
# strips only the space character, so it would miss tabs, newlines, and non-breaking spaces.
_BLANK = r"(?U)\A\s*\z"

# Helper column of the duplicate count; named so it can't collide with a user's column
_DUPLICATE_COUNT = "__pyspark_contracts_duplicate_count"


def _quote(col_name: str) -> str:
    """``col_name`` as one backtick-quoted identifier.

    Without quoting, Spark reads a dot in a name as a nested field (``a.b`` is field ``b`` of
    column ``a``), so a flat column literally named ``a.b`` — what Glue's ``relationalize``
    produces — can't be referenced.
    """
    return "`" + col_name.replace("`", "``") + "`"


class _QualityCheckMixin:
    def _samples(self, df: DataFrame, col_name: str, condition) -> tuple[list, list | None]:
        """Up to five failing values of ``col_name`` and, if the contract declares a ``key``,
        the key columns of those same rows — from one query, so they line up."""
        key = self.key
        selected = [F.col(_quote(col_name)).alias("_value")]
        selected += [F.col(_quote(k)).alias(f"_key{i}") for i, k in enumerate(key)]
        rows = df.filter(condition).select(*selected).limit(5).collect()
        values = [row["_value"] for row in rows]
        if not key:
            return values, None
        return values, [{k: row[f"_key{i}"] for i, k in enumerate(key)} for row in rows]

    def _row_rules(self, skip_columns: set, blocks_cross_column: bool) -> list[_RowRule]:
        """Every row-level constraint of this contract, in evaluation order.

        Per field, in declaration order: nullable, min/max value, min/max length, regex,
        allowed values, unique, condition; then each ``unique_together`` group. Fields in
        ``skip_columns`` (missing or mistyped) are left out — and groups containing one — and
        so are ``condition`` rules when ``blocks_cross_column`` is set.
        """
        rules: list[_RowRule] = []

        for col_name, field in self._fields.items():
            if col_name in skip_columns or not field._has_quality_constraints():
                continue
            col = F.col(_quote(col_name))
            first_rule = len(rules)

            if not field.nullable:
                if field.blank_is_null:
                    missing = col.isNull() | col.rlike(_BLANK)
                    constraint = "nullable, blank counts as null"
                else:
                    missing, constraint = col.isNull(), "nullable"
                rules.append(_RowRule(ViolationKind.NULL_VIOLATION, col_name, constraint, missing))
            if field.min_value is not None:
                rules.append(
                    _RowRule(
                        ViolationKind.VALUE_OUT_OF_RANGE,
                        col_name,
                        f"min_value={field.min_value}",
                        col < field.min_value,
                    )
                )
            if field.max_value is not None:
                rules.append(
                    _RowRule(
                        ViolationKind.VALUE_OUT_OF_RANGE,
                        col_name,
                        f"max_value={field.max_value}",
                        col > field.max_value,
                    )
                )
            if field.min_length is not None:
                rules.append(
                    _RowRule(
                        ViolationKind.LENGTH_OUT_OF_RANGE,
                        col_name,
                        f"min_length={field.min_length}",
                        F.length(col) < field.min_length,
                    )
                )
            if field.max_length is not None:
                rules.append(
                    _RowRule(
                        ViolationKind.LENGTH_OUT_OF_RANGE,
                        col_name,
                        f"max_length={field.max_length}",
                        F.length(col) > field.max_length,
                    )
                )
            if field.regex is not None:
                # rlike matches anywhere in the value; \A...\z anchors it to the whole value
                # (unlike ^...$, $ would also accept a trailing newline)
                rules.append(
                    _RowRule(
                        ViolationKind.REGEX_MISMATCH,
                        col_name,
                        f"regex={field.regex!r}",
                        ~col.rlike(rf"\A(?:{field.regex})\z"),
                    )
                )
            if field.allowed_values is not None:
                rules.append(
                    _RowRule(
                        ViolationKind.VALUE_NOT_ALLOWED,
                        col_name,
                        f"allowed_values={field.allowed_values}",
                        ~col.isin(field.allowed_values),
                    )
                )
            if field.unique:
                rules.append(_RowRule(ViolationKind.DUPLICATE_VALUE, col_name, "unique"))
            if field.condition is not None and not blocks_cross_column:
                rules.append(
                    _RowRule(
                        ViolationKind.CONDITION_FAILED,
                        col_name,
                        field.condition_description or "condition",
                        ~field.condition(col_name),
                    )
                )

            for rule in rules[first_rule:]:
                rule.severity = field.severity
                rule.max_failure_pct = field.max_failure_pct

        for group in self.unique_together:
            if any(column in skip_columns for column in group):
                continue
            constraint = f"unique_together=({', '.join(group)})"
            rules.append(
                _RowRule(ViolationKind.DUPLICATE_VALUE, None, constraint, columns=tuple(group))
            )

        return rules

    def _count_rows_and_failures(
        self, df: DataFrame, rules: list[_RowRule]
    ) -> tuple[int, list[int | None]]:
        """The row count and each rule's failing-row count, in a single Spark action.

        ``F.count(F.when(fail, 1))`` counts exactly the rows ``df.filter(fail).count()``
        would — a row whose predicate is null is in neither. ``unique`` and ``unique_together``
        rules get ``None``.
        """
        counted = [i for i, rule in enumerate(rules) if rule.fail is not None]
        row = df.agg(
            F.count(F.lit(1)).alias("_rows"),
            *[F.count(F.when(rules[i].fail, 1)).alias(f"_rule{i}") for i in counted],
        ).collect()[0]

        fail_counts: list[int | None] = [None] * len(rules)
        for i in counted:
            fail_counts[i] = row[f"_rule{i}"]
        return row["_rows"], fail_counts

    def _check_quality(
        self,
        df: DataFrame,
        row_count: int,
        rules: list[_RowRule],
        fail_counts: list[int | None],
        lazy: bool = True,
    ) -> list[Violation]:
        violations: list[Violation] = []

        for rule, fail in zip(rules, fail_counts):
            if rule.kind is ViolationKind.DUPLICATE_VALUE:
                violation = self._duplicate_violation(df, rule, row_count)
            elif fail:
                values, keys = self._samples(df, rule.column, rule.fail)
                violation = Violation(
                    kind=rule.kind,
                    severity=rule.severity_for(fail, row_count),
                    column=rule.column,
                    constraint=rule.constraint,
                    row_pct=round(fail / row_count * 100, 1),
                    failure_count=fail,
                    sample_values=values,
                    sample_keys=keys,
                )
            else:
                violation = None

            if violation is not None:
                violations.append(violation)
                if not lazy and violation.severity is Severity.ERROR:
                    return violations

        return violations

    def _duplicate_violation(
        self, df: DataFrame, rule: _RowRule, row_count: int
    ) -> Violation | None:
        columns = rule.columns or (rule.column,)
        quoted = [_quote(c) for c in columns]
        duplicate_groups = (
            df.filter(reduce(and_, [F.col(q).isNotNull() for q in quoted]))
            .groupBy(*quoted)
            .agg(F.count(F.lit(1)).alias(_DUPLICATE_COUNT))
            .filter(F.col(_DUPLICATE_COUNT) > 1)
        )
        fail = duplicate_groups.agg(F.sum(_DUPLICATE_COUNT)).collect()[0][0] or 0
        if not fail:
            return None
        rows = duplicate_groups.select(*quoted).limit(5).collect()
        if len(columns) == 1:
            sample = [row[0] for row in rows]
        else:
            sample = [{c: row[i] for i, c in enumerate(columns)} for row in rows]
        return Violation(
            kind=ViolationKind.DUPLICATE_VALUE,
            severity=rule.severity_for(fail, row_count),
            column=rule.column,
            constraint=rule.constraint,
            row_pct=round(fail / row_count * 100, 1),
            failure_count=fail,
            sample_values=sample,
        )
