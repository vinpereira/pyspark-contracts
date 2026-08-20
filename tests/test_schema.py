from pyspark.sql.types import FloatType, StringType

from pyspark_contracts._contract import Contract
from pyspark_contracts._field import Field


def test_contract_metaclass_collects_fields():
    class OdometerContract(Contract):
        vin = Field(StringType())
        odometer = Field(FloatType())

    assert "vin" in OdometerContract._fields
    assert "odometer" in OdometerContract._fields
    assert isinstance(OdometerContract._fields["vin"].dtype, StringType)
