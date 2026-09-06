from pyspark.sql.types import DataType, StructField, StructType


def dtype_to_json(dtype: DataType):
    return StructType([StructField("_", dtype)]).jsonValue()["fields"][0]["type"]


def dtype_from_json(type_json) -> DataType:
    wrapper = {
        "type": "struct",
        "fields": [{"name": "_", "type": type_json, "nullable": True, "metadata": {}}],
    }
    return StructType.fromJson(wrapper).fields[0].dataType
