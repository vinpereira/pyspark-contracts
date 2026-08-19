from pyspark.sql.types import FloatType, StringType

from pyspark_contracts._field import Field


def test_field_stores_type():
    f = Field(StringType())
    assert isinstance(f.dtype, StringType)


def test_field_nullable_defaults_to_true():
    f = Field(StringType())
    assert f.nullable is True


def test_field_nullable_can_be_false():
    f = Field(StringType(), nullable=False)
    assert f.nullable is False


def test_field_has_no_quality_constraints_by_default():
    f = Field(FloatType())
    assert not f.has_quality_constraints()


def test_field_min_value_sets_quality_constraint():
    f = Field(FloatType(), min_value=0)
    assert f.has_quality_constraints()
    assert f.min_value == 0


def test_field_regex_sets_quality_constraint():
    f = Field(StringType(), regex=r"^[A-Z0-9]{17}$")
    assert f.has_quality_constraints()
    assert f.regex == r"^[A-Z0-9]{17}$"


def test_field_allowed_values_sets_quality_constraint():
    f = Field(StringType(), allowed_values=["active", "inactive"])
    assert f.has_quality_constraints()
    assert f.allowed_values == ["active", "inactive"]
