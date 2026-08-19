import json
import logging
import sys

_LOG_RECORD_DEFAULTS = frozenset(vars(logging.LogRecord("", 0, "", 0, "", (), None)))


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "level": record.levelname,
            "message": record.getMessage(),
        }
        extras = {
            k: v
            for k, v in record.__dict__.items()
            if k not in _LOG_RECORD_DEFAULTS and not k.startswith("_")
        }
        entry.update(extras)
        return json.dumps(entry, default=str)


def get_logger(name: str = "pyspark_contracts") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
    return logger
