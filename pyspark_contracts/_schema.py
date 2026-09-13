import json
import warnings
from collections.abc import Callable

from pyspark_contracts._custom_checks import _CustomCheckMixin
from pyspark_contracts._dataset_checks import _DatasetCheckMixin
from pyspark_contracts._dtype_json import dtype_from_json
from pyspark_contracts._field import Field
from pyspark_contracts._quality_checks import _QualityCheckMixin
from pyspark_contracts._schema_checks import _SchemaCheckMixin
from pyspark_contracts._validation import _ValidationMixin


class ContractSchema(
    _ValidationMixin, _SchemaCheckMixin, _QualityCheckMixin, _CustomCheckMixin, _DatasetCheckMixin
):
    """Validates a DataFrame from an exported schema, without importing the original
    :class:`.Contract` subclass.

    Reconstructs everything :meth:`.Contract.to_dict` captured as data — types,
    nullability, and every quality constraint (including ``min_rows``/``max_rows``) —
    and shares the exact same ``validate()`` implementation ``Contract`` uses, so a
    reloaded schema enforces the same rules the original class did. It cannot
    reconstruct ``@check`` methods or ``Field(condition=...)`` lambdas, since those are
    arbitrary Python code that was never serialized — :meth:`from_dict`/
    :meth:`from_json` emit a ``UserWarning`` naming any such rules the original
    contract had, so you know they won't be enforced.

    Construct via :meth:`from_dict` or :meth:`from_json` — the constructor itself is
    not the intended entry point.
    """

    def __init__(
        self,
        contract_name: str,
        fields: dict[str, Field],
        min_rows: int | None = None,
        max_rows: int | None = None,
    ) -> None:
        self._name = contract_name
        self._fields = fields
        self._checks: dict[str, Callable] = {}
        self.min_rows = min_rows
        self.max_rows = max_rows

    def _report_name(self) -> str:
        return self._name

    @classmethod
    def from_dict(cls, schema_dict: dict) -> "ContractSchema":
        """Builds a ``ContractSchema`` from a dict produced by
        :meth:`.Contract.to_dict`.

        Args:
            schema_dict: The dict returned by ``SomeContract.to_dict()``.

        Returns:
            A ``ContractSchema`` whose ``.validate(df, ...)`` enforces the same
            structural and data constraints ``SomeContract`` did.

        Warns:
            UserWarning: If ``schema_dict`` includes any ``@check`` or
                ``condition_description`` entries — these rules exist in the
                original contract but have no executable logic to re-run here.
        """
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
                unique=entry.get("unique", False),
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

        return cls(
            schema_dict["contract"],
            fields,
            min_rows=schema_dict.get("min_rows"),
            max_rows=schema_dict.get("max_rows"),
        )

    @classmethod
    def from_json(cls, json_str: str) -> "ContractSchema":
        """Same as :meth:`from_dict`, from a JSON string (e.g. from
        :meth:`.Contract.to_json`) instead of a dict.
        """
        return cls.from_dict(json.loads(json_str))
