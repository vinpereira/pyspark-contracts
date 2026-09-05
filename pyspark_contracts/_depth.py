import enum


class ValidationDepth(enum.Enum):
    SCHEMA_ONLY = "schema_only"
    DATA_ONLY = "data_only"
    SCHEMA_AND_DATA = "schema_and_data"
