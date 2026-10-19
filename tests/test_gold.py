"""Unit tests for the gold candle maths on a small static trades DataFrame."""

from __future__ import annotations

import math
from datetime import datetime
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.types import (
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from crypto_pipeline.gold import aggregate_candles, to_ohlc, to_volatility

TRADES_SCHEMA = StructType(
    [
        StructField("symbol", StringType()),
        StructField("trade_id", LongType()),
        StructField("price", DecimalType(18, 8)),
        StructField("qty", DecimalType(18, 8)),
        StructField("quote_qty", DecimalType(38, 8)),
        StructField("trade_time", TimestampType()),
    ]
)


def trade(symbol: str, trade_id: int, ts: str, price: str, qty: str) -> tuple:
    p, q = Decimal(price), Decimal(qty)
    return (symbol, trade_id, p, q, p * q, datetime.fromisoformat(f"2026-01-01 {ts}"))


@pytest.fixture(scope="module")
def trades(spark: SparkSession) -> DataFrame:
    """BTC minute 12:00 (shuffled, with a same-millisecond tie), BTC 12:01, ETH 12:00."""
    rows = [
        trade("BTCUSDT", 3, "12:00:40", "90", "1"),
        trade("BTCUSDT", 1, "12:00:05", "100", "1"),
        trade("BTCUSDT", 4, "12:00:40", "105", "2"),
        trade("BTCUSDT", 2, "12:00:20", "110", "2"),
        trade("BTCUSDT", 5, "12:01:10", "200", "0.5"),
        trade("ETHUSDT", 1, "12:00:30", "50", "2"),
    ]
    return spark.createDataFrame(rows, TRADES_SCHEMA)


def candle(df: DataFrame, symbol: str, minute: str) -> Row:
    rows = df.filter(f"symbol = '{symbol}' AND minute = timestamp'2026-01-01 {minute}'").collect()
    assert len(rows) == 1
    return rows[0]


def test_one_candle_per_symbol_and_minute(trades: DataFrame) -> None:
    assert aggregate_candles(trades).count() == 3


def test_ohlc_prices_use_time_then_trade_id_ordering(trades: DataFrame) -> None:
    row = candle(to_ohlc(aggregate_candles(trades)), "BTCUSDT", "12:00:00")
    assert (row.open, row.high, row.low, row.close) == tuple(Decimal(x) for x in (100, 110, 90, 105))


def test_volume_trade_count_and_vwap(trades: DataFrame) -> None:
    row = candle(to_ohlc(aggregate_candles(trades)), "BTCUSDT", "12:00:00")
    assert row.volume == Decimal("6")
    assert row.quote_volume == Decimal("620")
    assert row.trade_count == 4
    assert row.vwap == Decimal("103.33333333")


def test_volatility_metrics(trades: DataFrame) -> None:
    row = candle(to_volatility(aggregate_candles(trades)), "BTCUSDT", "12:00:00")
    assert row.price_range == Decimal("20")
    assert row.price_stddev == pytest.approx(math.sqrt(54.6875))
    assert row.return_pct == pytest.approx(0.05)
    assert row.tick_count == 4


def test_single_trade_candle_has_zero_volatility(trades: DataFrame) -> None:
    row = candle(to_volatility(aggregate_candles(trades)), "BTCUSDT", "12:01:00")
    assert (row.price_range, row.price_stddev, row.return_pct, row.tick_count) == (Decimal("0"), 0.0, 0.0, 1)


def test_symbols_are_aggregated_separately(trades: DataFrame) -> None:
    row = candle(to_ohlc(aggregate_candles(trades)), "ETHUSDT", "12:00:00")
    assert (row.open, row.close, row.trade_count) == (Decimal("50"), Decimal("50"), 1)
