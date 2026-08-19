from pyspark_contracts._report import ContractViolationError, Violation, ViolationReport


def test_violation_report_falsy_when_empty():
    report = ViolationReport("MyContract", [], 100)
    assert not report


def test_violation_report_is_empty_when_empty():
    report = ViolationReport("MyContract", [], 100)
    assert report.is_empty() is True


def test_violation_report_truthy_when_has_violations():
    v = Violation(kind="missing_column", column="vin", expected_type="StringType")
    report = ViolationReport("MyContract", [v], 100)
    assert report


def test_violation_report_not_empty_when_has_violations():
    v = Violation(kind="missing_column", column="vin", expected_type="StringType")
    report = ViolationReport("MyContract", [v], 100)
    assert report.is_empty() is False


def test_violation_report_to_dict_structure():
    v = Violation(kind="missing_column", column="vin", expected_type="StringType")
    report = ViolationReport("MyContract", [v], 100, mode="soft")
    d = report.to_dict()
    assert d["contract"] == "MyContract"
    assert d["mode"] == "soft"
    assert d["row_count"] == 100
    assert len(d["violations"]) == 1
    assert d["violations"][0]["kind"] == "missing_column"
    assert d["violations"][0]["column"] == "vin"


def test_violation_report_to_dict_omits_none_fields():
    v = Violation(kind="missing_column", column="vin", expected_type="StringType")
    report = ViolationReport("MyContract", [v], 100)
    d = report.to_dict()
    violation = d["violations"][0]
    assert "actual_type" not in violation or violation.get("actual_type") is None


def test_contract_violation_error_has_report():
    v = Violation(kind="missing_column", column="vin", expected_type="StringType")
    report = ViolationReport("MyContract", [v], 100)
    err = ContractViolationError(report)
    assert err.report is report


def test_contract_violation_error_message_contains_contract_name():
    v = Violation(kind="missing_column", column="vin", expected_type="StringType")
    report = ViolationReport("MyContract", [v], 100)
    err = ContractViolationError(report)
    assert "MyContract" in str(err)
    assert "1 violation" in str(err)
