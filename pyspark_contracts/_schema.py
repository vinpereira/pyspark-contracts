import json
import warnings
from collections.abc import Callable

from pyspark_contracts._custom_checks import _CustomCheckMixin
from pyspark_contracts._dtype_json import dtype_from_json
from pyspark_contracts._field import Field
from pyspark_contracts._quality_checks import _QualityCheckMixin
from pyspark_contracts._schema_checks import _SchemaCheckMixin
from pyspark_contracts._validation import _ValidationMixin


class ContractSchema(_ValidationMixin, _SchemaCheckMixin, _QualityCheckMixin, _CustomCheckMixin):
    def __init__(self, contract_name: str, fields: dict[str, Field]) -> None:
        self._name = contract_name
        self._fields = fields
        self._checks: dict[str, Callable] = {}

    def _report_name(self) -> str:
        return self._name

    @classmethod
    def from_dict(cls, schema_dict: dict) -> "ContractSchema":
        fields: dict[str, Field] = {}
        for name, entry in schema_dict["fields"].items():
            fields[name] = Field(
                dtype_from_json(entry["type_json"]),
                nullable=entry.get("nullable", True),
                min_value=entry.get("min_value"),
                max_value=entry.get("max_value"),
                min_length=entry.get("min_length"),
                max_length=entry.get("max_length"),
                regex=entry.get("regex"),
                allowed_values=entry.get("allowed_values"),
                description=entry.get("description"),
                metadata=entry.get("metadata"),
            )

        unenforceable = list(schema_dict.get("checks", {}).keys())
        unenforceable += [
            name
            for name, entry in schema_dict["fields"].items()
            if "condition_description" in entry
        ]
        if unenforceable:
            warnings.warn(
                f"ContractSchema cannot re-run these rules from "
                f"'{schema_dict['contract']}' (the original @check/condition code isn't "
                f"part of the export, only its description): {unenforceable}",
                stacklevel=2,
            )

        return cls(schema_dict["contract"], fields)

    @classmethod
    def from_json(cls, json_str: str) -> "ContractSchema":
        return cls.from_dict(json.loads(json_str))
