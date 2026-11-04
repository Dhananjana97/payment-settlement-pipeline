"""Structured JSON logging.

    log = get_logger("reader", instance="host:123")
    log.info("session up", session_epoch=17, channel="M-0001")
    -> {"ts": "...", "level": "INFO", "service": "reader", "instance": "host:123",
        "msg": "session up", "session_epoch": 17, "channel": "M-0001"}

Correlation fields the assignment asks for (`event_id`, `payout_id`, `session_epoch`) are just
kwargs; `bind()` returns a child logger that carries fields on every record.
"""
from __future__ import annotations

import json
import logging
import sys
import time
from typing import Any

_CONFIGURED = False


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        extra = getattr(record, "fields", None)
        if extra:
            payload.update(extra)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, separators=(",", ":"))


def configure(level: str = "INFO") -> None:
    global _CONFIGURED
    root = logging.getLogger()
    if not _CONFIGURED:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(_JsonFormatter())
        root.handlers[:] = [handler]
        _CONFIGURED = True
    root.setLevel(level.upper())


class BoundLogger:
    def __init__(self, logger: logging.Logger, fields: dict[str, Any]):
        self._logger = logger
        self._fields = fields

    def bind(self, **fields: Any) -> "BoundLogger":
        return BoundLogger(self._logger, {**self._fields, **fields})

    def _log(self, level: int, msg: str, exc_info: bool, fields: dict[str, Any]) -> None:
        if self._logger.isEnabledFor(level):
            self._logger.log(level, msg, exc_info=exc_info, extra={"fields": {**self._fields, **fields}})

    def debug(self, msg: str, **fields: Any) -> None:
        self._log(logging.DEBUG, msg, False, fields)

    def info(self, msg: str, **fields: Any) -> None:
        self._log(logging.INFO, msg, False, fields)

    def warning(self, msg: str, **fields: Any) -> None:
        self._log(logging.WARNING, msg, False, fields)

    def error(self, msg: str, exc_info: bool = False, **fields: Any) -> None:
        self._log(logging.ERROR, msg, exc_info, fields)


def get_logger(service: str, **fields: Any) -> BoundLogger:
    if not _CONFIGURED:
        configure()
    return BoundLogger(logging.getLogger(service), {"service": service, **fields})
