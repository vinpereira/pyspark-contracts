import logging

from pyspark.sql import DataFrame

from pyspark_contracts._field import Field
from pyspark_contracts._report import ViolationReport


class ContractMeta(type):
    def __new__(mcs, name, bases, namespace):
        fields: dict[str, Field] = {}
        for attr_name, value in namespace.items():
            if isinstance(value, Field):
                fields[attr_name] = value
        namespace["_fields"] = fields
        return super().__new__(mcs, name, bases, namespace)


class Contract(metaclass=ContractMeta):
    _fields: dict[str, Field]

    def validate(
        self,
        df: DataFrame,
        *,
        mode: str = "hard",
        logger: logging.Logger | None = None,
    ) -> ViolationReport:
        raise NotImplementedError
