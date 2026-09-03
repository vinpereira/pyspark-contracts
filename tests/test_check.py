from pyspark.sql.types import FloatType

from pyspark_contracts._check import check
from pyspark_contracts._contract import Contract
from pyspark_contracts._field import Field


def test_check_decorator_stores_description():
    @check("must be positive")
    def my_check(self, df):
        return df

    assert my_check._check_description == "must be positive"


def test_contract_metaclass_collects_checks():
    class MyContract(Contract):
        odometer = Field(FloatType())

        @check("odometer must be non-negative")
        def no_negative(self, df):
            return df.filter(df.odometer < 0)

    assert "no_negative" in MyContract._checks
    assert MyContract._checks["no_negative"]._check_description == ("odometer must be non-negative")
    assert "odometer" not in MyContract._checks
