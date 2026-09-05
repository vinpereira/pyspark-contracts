from pyspark_contracts._depth import ValidationDepth


def test_validation_depth_has_three_members():
    assert ValidationDepth.SCHEMA_ONLY.value == "schema_only"
    assert ValidationDepth.DATA_ONLY.value == "data_only"
    assert ValidationDepth.SCHEMA_AND_DATA.value == "schema_and_data"
