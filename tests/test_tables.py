"""Unit tests for the shutdown helpers in tables.py."""

from __future__ import annotations

import logging
import sys
import time

import pytest

from crypto_pipeline import tables
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


class _FakeQuery:
    def __init__(self, name: str, error: Exception | None = None) -> None:
        self.name, self._error = name, error

    def exception(self) -> Exception | None:
        return self._error


class _FakeSpark:
    def __init__(self, queries: list[_FakeQuery]) -> None:
        self.streams = type("Streams", (), {"active": queries})()


def test_wait_for_table_returns_once_upstream_table_exists(monkeypatch) -> None:
    calls = iter([False, False, True])
    monkeypatch.setattr(tables, "table_exists", lambda *args: next(calls))
    monkeypatch.setattr(tables.time, "sleep", lambda seconds: None)

    tables.wait_for_table(_FakeSpark([_FakeQuery("bronze_trades_raw")]), None, "bronze", "trades_raw")


def test_wait_for_table_raises_if_upstream_query_failed(monkeypatch) -> None:
    monkeypatch.setattr(tables, "table_exists", lambda *args: False)
    monkeypatch.setattr(tables.time, "sleep", lambda seconds: None)
    spark = _FakeSpark([_FakeQuery("bronze_trades_raw", RuntimeError("kafka down"))])

    with pytest.raises(RuntimeError, match="bronze_trades_raw failed"):
        tables.wait_for_table(spark, None, "bronze", "trades_raw")


def test_wait_for_table_times_out(monkeypatch) -> None:
    monkeypatch.setattr(tables, "table_exists", lambda *args: False)
    monkeypatch.setattr(tables.time, "sleep", lambda seconds: None)

    with pytest.raises(TimeoutError, match="silver.trades"):
        tables.wait_for_table(_FakeSpark([]), None, "silver", "trades", timeout_seconds=0)
