import json

from pyspark_contracts._dtype_json import dtype_to_json


class _DocumentationMixin:
    @classmethod
    def to_dict(cls) -> dict:
        """Exports this contract's full shape as plain data.

        Includes every field's type (both a human-readable ``simpleString()`` and a
        machine-readable ``type_json`` for exact reconstruction), nullability, every
        quality constraint that's set, ``description``/``metadata``, the
        name/description of every ``@check`` method, and ``min_rows``/``max_rows`` if
        set. Comprehensive enough to work as a data dictionary or to share a
        contract's shape without importing the Python class — see
        :meth:`.ContractSchema.from_dict`.

        ``@check`` methods and ``Field(condition=...)`` lambdas are arbitrary Python
        code and can never be serialized — only their description text is included,
        never the executable logic.

        Returns:
            A dict shaped ``{"contract": ..., "fields": {...}, "checks": {...}}``,
            with ``"min_rows"``/``"max_rows"`` included only when set.
        """
        fields: dict[str, dict] = {}
        for name, field in cls._fields.items():
            entry: dict = {
                "type": field.dtype.simpleString(),
                "type_json": dtype_to_json(field.dtype),
                "nullable": field.nullable,
            }
            if field.min_value is not None:
                entry["min_value"] = field.min_value
            if field.max_value is not None:
                entry["max_value"] = field.max_value
            if field.min_length is not None:
                entry["min_length"] = field.min_length
            if field.max_length is not None:
                entry["max_length"] = field.max_length
            if field.regex is not None:
                entry["regex"] = field.regex
            if field.allowed_values is not None:
                entry["allowed_values"] = field.allowed_values
            if field.condition_description is not None:
                entry["condition_description"] = field.condition_description
            if field.unique:
                entry["unique"] = True
            if field.description is not None:
                entry["description"] = field.description
            if field.metadata is not None:
                entry["metadata"] = field.metadata
            fields[name] = entry

        checks = {name: method._check_description for name, method in cls._checks.items()}

        result = {
            "contract": cls.__name__,
            "fields": fields,
            "checks": checks,
        }
        if cls.min_rows is not None:
            result["min_rows"] = cls.min_rows
        if cls.max_rows is not None:
            result["max_rows"] = cls.max_rows
        return result

    @classmethod
    def to_json(cls) -> str:
        """Same as :meth:`to_dict`, serialized to an indented JSON string."""
        return json.dumps(cls.to_dict(), indent=2)

    @classmethod
    def describe(cls) -> str:
        """Returns a human-readable, printable summary of this contract.

        One line per field (name, type, nullability, active constraints,
        description, metadata), followed by ``min_rows``/``max_rows`` if set and a
        ``Checks:`` section listing every ``@check``'s name and description, if any.
        The exact column alignment isn't part of this method's contract — only the
        information it contains is.

        Example:
            >>> print(MyContract.describe())
        """
        lines = [cls.__name__]
        name_width = max((len(n) for n in cls._fields), default=0)
        type_width = max((len(f.dtype.simpleString()) for f in cls._fields.values()), default=0)

        for name, field in cls._fields.items():
            nullability = "nullable" if field.nullable else "not null"
            constraints = []
            if field.min_value is not None:
                constraints.append(f"min={field.min_value}")
            if field.max_value is not None:
                constraints.append(f"max={field.max_value}")
            if field.min_length is not None:
                constraints.append(f"min_length={field.min_length}")
            if field.max_length is not None:
                constraints.append(f"max_length={field.max_length}")
            if field.regex is not None:
                constraints.append(f"regex={field.regex!r}")
            if field.allowed_values is not None:
                constraints.append(f"allowed_values={field.allowed_values}")
            if field.condition_description is not None:
                constraints.append(f"condition={field.condition_description!r}")
            if field.unique:
                constraints.append("unique")

            parts = [
                f"  {name:<{name_width}}",
                f"{field.dtype.simpleString():<{type_width}}",
                f"{nullability:<8}",
            ]
            if constraints:
                parts.append(" ".join(constraints))
            if field.description:
                parts.append(field.description)
            if field.metadata:
                metadata_str = ", ".join(f"{k}={v}" for k, v in field.metadata.items())
                parts.append(f"[{metadata_str}]")

            lines.append("  ".join(parts))

        if cls.min_rows is not None or cls.max_rows is not None:
            bounds = []
            if cls.min_rows is not None:
                bounds.append(f"min_rows={cls.min_rows}")
            if cls.max_rows is not None:
                bounds.append(f"max_rows={cls.max_rows}")
            lines.append("")
            lines.append(f"Rows: {', '.join(bounds)}")

        if cls._checks:
            lines.append("")
            lines.append("Checks:")
            for check_name, method in cls._checks.items():
                lines.append(f"  {check_name}: {method._check_description}")

        return "\n".join(lines)
