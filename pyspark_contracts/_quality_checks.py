from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from pyspark_contracts._report import Violation, ViolationKind


@dataclass
class _RowRule:
    """One row-level constraint on one column.

    ``fail`` selects the rows that break it. ``unique`` rules have no ``fail``: they need a
    ``groupBy``, so they can't be part of the single aggregation and are counted on demand.
    """

    kind: ViolationKind
    column: str
    constraint: str
    fail: Column | None = None


class _QualityCheckMixin:
    def _sample_values(self, df: DataFrame, col_name: str, condition) -> list:
        rows = df.filter(condition).select(col_name).limit(5).collect()
        return [row[0] for row in rows]

    def _row_rules(self, skip_columns: set, blocks_cross_column: bool) -> list[_RowRule]:
        """Every row-level constraint of this contract, in evaluation order.

        Per field, in declaration order: nullable, min/max value, min/max length, regex,
        allowed values, unique, condition. Fields in ``skip_columns`` (missing or mistyped)
        are left out, and so are ``condition`` rules when ``blocks_cross_column`` is set.
        """
        rules: list[_RowRule] = []

        for col_name, field in self._fields.items():
            if col_name in skip_columns or not field._has_quality_constraints():
                continue
            col = F.col(col_name)

            if not field.nullable:
                rules.append(
                    _RowRule(ViolationKind.NULL_VIOLATION, col_name, "nullable", col.isNull())
                )
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

        return rules

    def _count_rows_and_failures(
        self, df: DataFrame, rules: list[_RowRule]
    ) -> tuple[int, list[int | None]]:
        """The row count and each rule's failing-row count, in a single Spark action.

        ``F.count(F.when(fail, 1))`` counts exactly the rows ``df.filter(fail).count()``
        would — a row whose predicate is null is in neither. ``unique`` rules get ``None``.
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
                violation = self._duplicate_violation(df, rule.column, row_count)
            elif fail:
                violation = Violation(
                    kind=rule.kind,
                    column=rule.column,
                    constraint=rule.constraint,
                    row_pct=round(fail / row_count * 100, 1),
                    failure_count=fail,
                    sample_values=self._sample_values(df, rule.column, rule.fail),
                )
            else:
                violation = None

            if violation is not None:
                violations.append(violation)
                if not lazy:
                    return violations

        return violations

    def _duplicate_violation(
        self, df: DataFrame, col_name: str, row_count: int
    ) -> Violation | None:
        duplicate_groups = (
            df.filter(F.col(col_name).isNotNull())
            .groupBy(col_name)
            .count()
            .filter(F.col("count") > 1)
        )
        fail = duplicate_groups.agg(F.sum("count")).collect()[0][0] or 0
        if not fail:
            return None
        sample = [row[col_name] for row in duplicate_groups.select(col_name).limit(5).collect()]
        return Violation(
            kind=ViolationKind.DUPLICATE_VALUE,
            column=col_name,
            constraint="unique",
            row_pct=round(fail / row_count * 100, 1),
            failure_count=fail,
            sample_values=sample,
        )
