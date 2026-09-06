from collections.abc import Callable

from pyspark_contracts._custom_checks import _CustomCheckMixin
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
    metaclass=ContractMeta,
):
    _fields: dict[str, Field]
    _checks: dict[str, Callable]
