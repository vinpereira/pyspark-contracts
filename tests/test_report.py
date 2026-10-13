import json

import pytest

from pyspark_contracts._depth import ValidationDepth
from pyspark_contracts._report import (
    ContractViolationError,
    Violation,
    ViolationKind,
    ViolationReport,
)


def test_violation_kind_compares_and_formats_as_its_string():
    assert ViolationKind.NULL_VIOLATION == "null_violation"
    assert str(ViolationKind.NULL_VIOLATION) == "null_violation"
    assert f"{ViolationKind.NULL_VIOLATION}" == "null_violation"


def test_violation_coerces_kind_string_to_violation_kind():
    v = Violation(kind="missing_column", column="vin")
    assert v.kind is ViolationKind.MISSING_COLUMN


def test_violation_rejects_unknown_kind():
    with pytest.raises(ValueError):
        Violation(kind="not_a_kind")


def test_violation_report_to_dict_emits_kind_as_plain_string():
    v = Violation(kind=ViolationKind.CHECK_FAILED, check="no_negative")
    violation = ViolationReport("MyContract", [v], 100).to_dict()["violations"][0]
    assert type(violation["kind"]) is str
    assert violation == {"kind": "check_failed", "check": "no_negative"}
    assert json.loads(json.dumps(violation))["kind"] == "check_failed"


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


def test_violation_report_depth_defaults_to_schema_and_data():
    report = ViolationReport("MyContract", [], 100)
    assert report.depth == ValidationDepth.SCHEMA_AND_DATA


def test_violation_report_to_dict_includes_depth():
    report = ViolationReport("MyContract", [], 100, depth=ValidationDepth.SCHEMA_ONLY)
    d = report.to_dict()
    assert d["depth"] == "schema_only"


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
