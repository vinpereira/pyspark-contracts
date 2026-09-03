from collections.abc import Callable

from pyspark.sql import Column
from pyspark.sql.types import DataType


class Field:
    def __init__(
        self,
        dtype: DataType,
        *,
        nullable: bool = True,
        min_value=None,
        max_value=None,
        min_length: int | None = None,
        max_length: int | None = None,
        regex: str | None = None,
        allowed_values: list | None = None,
        condition: Callable[[str], Column] | None = None,
        condition_description: str | None = None,
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

    def has_quality_constraints(self) -> bool:
        return any(
            [
                self.min_value is not None,
                self.max_value is not None,
                self.min_length is not None,
                self.max_length is not None,
                self.regex is not None,
                self.allowed_values is not None,
                self.condition is not None,
            ]
        )
