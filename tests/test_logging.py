import json
import uuid

from pyspark_contracts._logging import get_logger


def _unique_logger():
    return get_logger(f"test-{uuid.uuid4()}")


def test_logger_emits_json_with_level_and_message(capsys):
    logger = _unique_logger()
    logger.info("hello world")
    out = capsys.readouterr().out.strip()
    data = json.loads(out)
    assert data["level"] == "INFO"
    assert data["message"] == "hello world"


def test_logger_warning_sets_level(capsys):
    logger = _unique_logger()
    logger.warning("something wrong")
    out = capsys.readouterr().out.strip()
    data = json.loads(out)
    assert data["level"] == "WARNING"


def test_logger_includes_extra_fields(capsys):
    logger = _unique_logger()
    logger.info("done", extra={"contract": "MyContract", "row_count": 42})
    out = capsys.readouterr().out.strip()
    data = json.loads(out)
    assert data["contract"] == "MyContract"
    assert data["row_count"] == 42


def test_logger_second_call_does_not_duplicate_handlers(capsys):
    name = f"test-{uuid.uuid4()}"
    _ = get_logger(name)
    logger2 = get_logger(name)
    logger2.info("once")
    out = capsys.readouterr().out.strip()
    lines = [line for line in out.splitlines() if line.strip()]
    assert len(lines) == 1
