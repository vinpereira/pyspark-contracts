from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pyspark_contracts._report import Violation


class _QualityCheckMixin:
    def _sample_values(self, df: DataFrame, col_name: str, condition) -> list:
        rows = df.filter(condition).select(col_name).limit(5).collect()
        return [row[0] for row in rows]

    def _check_quality(
        self,
        df: DataFrame,
        row_count: int,
        skip_columns: set[str],
        lazy: bool = True,
        blocks_cross_column: bool = False,
    ) -> list[Violation]:
        violations: list[Violation] = []

        for col_name, field in self._fields.items():
            if col_name in skip_columns or not field._has_quality_constraints():
                continue

            if not field.nullable:
                condition = F.col(col_name).isNull()
                fail = df.filter(condition).count()
                if fail:
                    violations.append(
                        Violation(
                            kind="null_violation",
                            column=col_name,
                            constraint="nullable",
                            row_pct=round(fail / row_count * 100, 1),
                            failure_count=fail,
                            sample_values=self._sample_values(df, col_name, condition),
                        )
                    )
                    if not lazy:
                        return violations

            if field.min_value is not None:
                condition = F.col(col_name) < field.min_value
                fail = df.filter(condition).count()
                if fail:
                    violations.append(
                        Violation(
                            kind="value_out_of_range",
                            column=col_name,
                            constraint=f"min_value={field.min_value}",
                            row_pct=round(fail / row_count * 100, 1),
                            failure_count=fail,
                            sample_values=self._sample_values(df, col_name, condition),
                        )
                    )
                    if not lazy:
                        return violations

            if field.max_value is not None:
                condition = F.col(col_name) > field.max_value
                fail = df.filter(condition).count()
                if fail:
                    violations.append(
                        Violation(
                            kind="value_out_of_range",
                            column=col_name,
                            constraint=f"max_value={field.max_value}",
                            row_pct=round(fail / row_count * 100, 1),
                            failure_count=fail,
                            sample_values=self._sample_values(df, col_name, condition),
                        )
                    )
                    if not lazy:
                        return violations

            if field.min_length is not None:
                condition = F.length(F.col(col_name)) < field.min_length
                fail = df.filter(condition).count()
                if fail:
                    violations.append(
                        Violation(
                            kind="length_out_of_range",
                            column=col_name,
                            constraint=f"min_length={field.min_length}",
                            row_pct=round(fail / row_count * 100, 1),
                            failure_count=fail,
                            sample_values=self._sample_values(df, col_name, condition),
                        )
                    )
                    if not lazy:
                        return violations

            if field.max_length is not None:
                condition = F.length(F.col(col_name)) > field.max_length
                fail = df.filter(condition).count()
                if fail:
                    violations.append(
                        Violation(
                            kind="length_out_of_range",
                            column=col_name,
                            constraint=f"max_length={field.max_length}",
                            row_pct=round(fail / row_count * 100, 1),
                            failure_count=fail,
                            sample_values=self._sample_values(df, col_name, condition),
                        )
                    )
                    if not lazy:
                        return violations

            if field.regex is not None:
                condition = ~F.col(col_name).rlike(field.regex)
                fail = df.filter(condition).count()
                if fail:
                    violations.append(
                        Violation(
                            kind="regex_mismatch",
                            column=col_name,
                            constraint=f"regex={field.regex!r}",
                            row_pct=round(fail / row_count * 100, 1),
                            failure_count=fail,
                            sample_values=self._sample_values(df, col_name, condition),
                        )
                    )
                    if not lazy:
                        return violations

            if field.allowed_values is not None:
                condition = ~F.col(col_name).isin(field.allowed_values)
                fail = df.filter(condition).count()
                if fail:
                    violations.append(
                        Violation(
                            kind="value_not_allowed",
                            column=col_name,
                            constraint=f"allowed_values={field.allowed_values}",
                            row_pct=round(fail / row_count * 100, 1),
                            failure_count=fail,
                            sample_values=self._sample_values(df, col_name, condition),
                        )
                    )
                    if not lazy:
                        return violations

            if field.unique:
                duplicate_groups = (
                    df.filter(F.col(col_name).isNotNull())
                    .groupBy(col_name)
                    .count()
                    .filter(F.col("count") > 1)
                )
                fail = duplicate_groups.agg(F.sum("count")).collect()[0][0] or 0
                if fail:
                    sample = [
                        row[col_name]
                        for row in duplicate_groups.select(col_name).limit(5).collect()
                    ]
                    violations.append(
                        Violation(
                            kind="duplicate_value",
                            column=col_name,
                            constraint="unique",
                            row_pct=round(fail / row_count * 100, 1),
                            failure_count=fail,
                            sample_values=sample,
                        )
                    )
                    if not lazy:
                        return violations

            if field.condition is not None and not blocks_cross_column:
                condition = ~field.condition(col_name)
                fail = df.filter(condition).count()
                if fail:
                    violations.append(
                        Violation(
                            kind="condition_failed",
                            column=col_name,
                            constraint=field.condition_description or "condition",
                            row_pct=round(fail / row_count * 100, 1),
                            failure_count=fail,
                            sample_values=self._sample_values(df, col_name, condition),
                        )
                    )
                    if not lazy:
                        return violations

        return violations
