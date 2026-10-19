"""Explicit schemas and column types for the trade feed.

Binance WS trade payload: e event type, E event time (ms), s symbol, t trade id, p price (string),
q quantity (string), T trade time (ms), m is buyer maker, M ignore.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# from_json (PERMISSIVE) copies unparseable input here and leaves the other fields NULL.
CORRUPT_FIELD = "_corrupt_record"

BINANCE_TRADE_SCHEMA = StructType(
    [
        StructField("e", StringType()),
        StructField("E", LongType()),
        StructField("s", StringType()),
        StructField("t", LongType()),
        StructField("p", StringType()),
        StructField("q", StringType()),
        StructField("T", LongType()),
        StructField("m", BooleanType()),
        StructField("M", BooleanType()),
        StructField(CORRUPT_FIELD, StringType()),
    ]
)

PRICE_TYPE = DecimalType(18, 8)
QTY_TYPE = DecimalType(18, 8)
QUOTE_QTY_TYPE = DecimalType(38, 8)

VOLUME_TYPE = DecimalType(28, 8)

OHLC_SCHEMA = StructType(
    [
        StructField("symbol", StringType()),
        StructField("minute", TimestampType()),
        StructField("open", PRICE_TYPE),
        StructField("high", PRICE_TYPE),
        StructField("low", PRICE_TYPE),
        StructField("close", PRICE_TYPE),
        StructField("volume", VOLUME_TYPE),
        StructField("quote_volume", QUOTE_QTY_TYPE),
        StructField("trade_count", LongType()),
        StructField("vwap", PRICE_TYPE),
    ]
)

VOLATILITY_SCHEMA = StructType(
    [
        StructField("symbol", StringType()),
        StructField("minute", TimestampType()),
        StructField("price_range", PRICE_TYPE),
        StructField("price_stddev", DoubleType()),
        StructField("return_pct", DoubleType()),
        StructField("tick_count", LongType()),
    ]
)

PARSED_COL = "parsed"
REASON_COL = "reason"
