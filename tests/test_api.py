def test_public_imports():
    from pyspark_contracts import Contract, ContractViolationError, Field, ViolationReport

    assert Contract is not None
    assert Field is not None
    assert ContractViolationError is not None
    assert ViolationReport is not None


def test_version_is_defined():
    import pyspark_contracts

    assert hasattr(pyspark_contracts, "__version__")
    assert pyspark_contracts.__version__ == "0.1.0"
