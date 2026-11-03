from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from pyspark_contracts._custom_checks import _CustomCheckMixin
from pyspark_contracts._dataset_checks import _DatasetCheckMixin
from pyspark_contracts._declared import normalize_key, normalize_unique_together
from pyspark_contracts._documentation import _DocumentationMixin
from pyspark_contracts._field import Field
from pyspark_contracts._quality_checks import _QualityCheckMixin
from pyspark_contracts._schema_checks import _SchemaCheckMixin
from pyspark_contracts._split import _SplitMixin
from pyspark_contracts._struct_type import fields_to_struct_type
from pyspark_contracts._validation import _ValidationMixin


class ContractMeta(type):
    def __new__(mcs, name, bases, namespace):
        fields: dict[str, Field] = {}
        checks: dict[str, Callable] = {}
        for base in bases:
            fields.update(getattr(base, "_fields", {}))
            checks.update(getattr(base, "_checks", {}))
        declared: dict[str, str] = {}
        for attr_name, value in namespace.items():
            if isinstance(value, Field):
                column = value.column or attr_name
                if column in declared:
                    raise TypeError(
                        f"{name}: attributes '{declared[column]}' and '{attr_name}' both "
                        f"declare column '{column}'"
                    )
                declared[column] = attr_name
                fields[column] = value
            elif callable(value) and hasattr(value, "_check_description"):
                checks[attr_name] = value
        if "key" in namespace:
            namespace["key"] = normalize_key(name, namespace["key"], fields)
        if "unique_together" in namespace:
            namespace["unique_together"] = normalize_unique_together(
                name, namespace["unique_together"], fields
            )
        namespace["_fields"] = fields
        namespace["_checks"] = checks
        return super().__new__(mcs, name, bases, namespace)


class Contract(
    _ValidationMixin,
    _DocumentationMixin,
    _SchemaCheckMixin,
    _QualityCheckMixin,
    _CustomCheckMixin,
    _DatasetCheckMixin,
    _SplitMixin,
    metaclass=ContractMeta,
):
    """Base class for declaring a PySpark DataFrame schema and its data quality rules.

    Subclass it and declare :class:`.Field`\\ s as class attributes — each becomes
    one expected column:

    >>> from pyspark.sql.types import FloatType, StringType
    >>> class OdometerContract(Contract):
    ...     vin = Field(StringType(), nullable=False, min_length=17, max_length=17)
    ...     odometer_start = Field(FloatType(), nullable=False, min_value=0)

    Then call :meth:`~pyspark_contracts._validation._ValidationMixin.validate` on an
    instance to check a DataFrame against it. Subclasses also inherit fields and
    ``@check`` methods declared on their base ``Contract`` classes — a subclass can
    override an inherited field or check by redeclaring it under the same name.

    Optional class attributes ``min_rows``/``max_rows`` (plain ``int``, not
    ``Field``) assert something about the DataFrame as a whole rather than a single
    column — see the dataset-level checks section of the README.

    Set the class attribute ``strict = True`` to report every DataFrame column the
    contract doesn't declare as an ``unexpected_column`` violation. Off by default.

    ``key = ["a", "b"]`` names the columns that identify a row, so row-level violations
    carry ``sample_keys`` — which records failed. ``unique_together = [("a", "b")]``
    asserts that each combination of those columns occurs once. Both take declared
    column names (the real column, if a field sets ``column=``).

    :meth:`split` separates the rows that pass the row-level rules from those that don't,
    for jobs that quarantine bad rows instead of aborting.
    """

    _fields: dict[str, Field]
    _checks: dict[str, Callable]

    @classmethod
    def to_struct_type(cls) -> StructType:
        """The Spark schema this contract declares, in field declaration order.

        Use it to read data with the contract as its schema, so the declaration lives in
        one place:

        >>> df = spark.read.schema(OdometerContract.to_struct_type()).parquet(path)

        Each field's ``nullable`` carries over (Spark relaxes it to ``True`` when reading
        files, so ``validate()`` is still what enforces it), and its ``description``
        becomes the column's ``comment`` metadata. Only the structure is included — data
        constraints like ``min_value`` exist only in the contract.
        """
        return fields_to_struct_type(cls._fields)

    @classmethod
    def empty_df(cls, spark: SparkSession) -> DataFrame:
        """An empty DataFrame with this contract's schema — e.g. a test fixture for a
        pipeline step whose input is validated against this contract.

        Args:
            spark: The ``SparkSession`` to create the DataFrame with.
        """
        return spark.createDataFrame([], cls.to_struct_type())
