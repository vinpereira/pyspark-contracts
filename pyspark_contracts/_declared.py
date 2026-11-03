from collections.abc import Collection, Iterable


def normalize_key(owner: str, key: object, fields: Collection[str]) -> tuple[str, ...]:
    """``key`` as a tuple of declared column names.

    Raises ``TypeError`` if it isn't a list/tuple of strings or names an undeclared column.
    """
    if isinstance(key, str) or not isinstance(key, Iterable):
        raise TypeError(f"{owner}: key must be a list of column names, got {key!r}")
    columns = tuple(key)
    for column in columns:
        if not isinstance(column, str):
            raise TypeError(f"{owner}: key entries must be column names (strings), got {column!r}")
        if column not in fields:
            raise TypeError(f"{owner}: key column '{column}' is not a declared field")
    return columns


def normalize_unique_together(
    owner: str, groups: object, fields: Collection[str]
) -> tuple[tuple[str, ...], ...]:
    """``unique_together`` as a tuple of tuples of declared column names.

    Raises ``TypeError`` for a malformed value or an undeclared column, and ``ValueError`` for
    a group with fewer than two columns or a repeated column.
    """
    if isinstance(groups, str) or not isinstance(groups, Iterable):
        raise TypeError(
            f"{owner}: unique_together must be a list of column groups, "
            f"e.g. [('a', 'b')], got {groups!r}"
        )
    normalized = []
    for group in groups:
        if isinstance(group, str) or not isinstance(group, Iterable):
            raise TypeError(
                f"{owner}: unique_together must be a list of column groups, "
                f"e.g. [('a', 'b')]; got the entry {group!r}"
            )
        columns = tuple(group)
        for column in columns:
            if not isinstance(column, str):
                raise TypeError(
                    f"{owner}: unique_together entries must be column names (strings), "
                    f"got {column!r}"
                )
            if column not in fields:
                raise TypeError(
                    f"{owner}: unique_together column '{column}' is not a declared field"
                )
        if len(columns) < 2:
            raise ValueError(
                f"{owner}: unique_together group {columns!r} has fewer than two columns; "
                f"use Field(unique=True) for a single column"
            )
        if len(set(columns)) != len(columns):
            raise ValueError(f"{owner}: unique_together group {columns!r} repeats a column")
        normalized.append(columns)
    return tuple(normalized)
