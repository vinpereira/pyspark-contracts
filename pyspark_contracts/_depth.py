import enum


class ValidationDepth(enum.Enum):
    """Controls which stage(s) of :meth:`.Contract.validate` run.

    Passed as ``validate(df, depth=...)``. Defaults to ``SCHEMA_AND_DATA``.

    Attributes:
        SCHEMA_ONLY: Column names and types only — reads ``df.schema`` (metadata),
            triggering zero Spark actions. Particularly useful in unit tests where
            the DataFrame is built from a known factory: the schema is guaranteed by
            construction, so only the data constraints are worth checking elsewhere.
        DATA_ONLY: Skips structural checks (``missing_column``/``type_mismatch``) and
            runs only the data constraints (nullable, range, length, regex, allowed
            values, unique, condition, ``@check``, ``min_rows``/``max_rows``).
            Assumes the schema is already correct — if it isn't, Spark itself raises
            when a referenced column doesn't exist, since there's no structural
            safety net in this mode.
        SCHEMA_AND_DATA: Both — the full validation pipeline, exactly as if ``depth``
            were never passed.
    """

    SCHEMA_ONLY = "schema_only"
    DATA_ONLY = "data_only"
    SCHEMA_AND_DATA = "schema_and_data"
