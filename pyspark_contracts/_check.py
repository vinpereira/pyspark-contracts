from collections.abc import Callable


def check(description: str) -> Callable[[Callable], Callable]:
    """Marks a :class:`.Contract` method as a cross-column data quality check.

    The decorated method is collected automatically by ``Contract``'s metaclass — it
    doesn't need to be called directly. It must accept ``self`` and ``df`` (the full
    DataFrame being validated) and return the **failing rows**; an empty result means
    the check passed. Extra parameters are supplied via ``validate(df, **kwargs)`` and
    routed only to the checks that declare them.

    Skipped entirely when the DataFrame has any ``missing_column``/``type_mismatch``
    violation, since the method may reference arbitrary columns.

    Args:
        description: Human-readable text describing what the check verifies (e.g.
            ``"delta_km must be non-negative"``). Used as the resulting
            ``check_failed`` violation's ``constraint`` text, and exported by
            ``to_dict()``/``describe()`` — the check's code itself is never
            serialized.

    Example:
        >>> class FinalDfContract(Contract):
        ...     @check("delta_km must be non-negative")
        ...     def no_regression(self, df):
        ...         return df.filter(F.col("delta_km") < 0)
    """

    def decorator(func: Callable) -> Callable:
        func._check_description = description
        return func

    return decorator
