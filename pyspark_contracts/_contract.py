from collections.abc import Callable

from pyspark_contracts._custom_checks import _CustomCheckMixin
from pyspark_contracts._dataset_checks import _DatasetCheckMixin
from pyspark_contracts._documentation import _DocumentationMixin
from pyspark_contracts._field import Field
from pyspark_contracts._quality_checks import _QualityCheckMixin
from pyspark_contracts._schema_checks import _SchemaCheckMixin
from pyspark_contracts._validation import _ValidationMixin


class ContractMeta(type):
    def __new__(mcs, name, bases, namespace):
        fields: dict[str, Field] = {}
        checks: dict[str, Callable] = {}
        for base in bases:
            fields.update(getattr(base, "_fields", {}))
            checks.update(getattr(base, "_checks", {}))
        for attr_name, value in namespace.items():
            if isinstance(value, Field):
                fields[attr_name] = value
            elif callable(value) and hasattr(value, "_check_description"):
                checks[attr_name] = value
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
    """

    _fields: dict[str, Field]
    _checks: dict[str, Callable]
