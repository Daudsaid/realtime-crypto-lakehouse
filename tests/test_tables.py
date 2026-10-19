"""Unit tests for the shutdown helpers in tables.py."""

from __future__ import annotations

import logging
import sys
import time

from crypto_pipeline.tables import ExpectedShutdownErrors, _StopRequest


def make_record() -> logging.LogRecord:
    try:
        raise RuntimeError("batch cancelled")
    except RuntimeError:
        return logging.LogRecord(
            "py4j.clientserver", logging.ERROR, "x.py", 1, "Python Proxy failed", (), sys.exc_info()
        )


def test_error_is_downgraded_to_warning_after_stop_requested() -> None:
    stop = _StopRequest()
    stop.requested_at = time.monotonic()
    record = make_record()

    assert ExpectedShutdownErrors(stop).filter(record)
    assert record.levelno == logging.WARNING
    assert record.levelname == "WARNING"
    assert record.exc_info is None
    assert "expected during shutdown" in record.getMessage()


def test_error_is_kept_while_running() -> None:
    record = make_record()

    assert ExpectedShutdownErrors(_StopRequest()).filter(record)
    assert record.levelno == logging.ERROR
    assert record.exc_info is not None


def test_stop_request_is_lock_free_and_keeps_first_timestamp() -> None:
    stop = _StopRequest()
    stop(2, None)
    first = stop.requested_at
    stop(2, None)
    assert first is not None
    assert stop.requested_at == first
