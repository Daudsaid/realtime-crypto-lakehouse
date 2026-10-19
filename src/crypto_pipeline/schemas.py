"""Explicit schemas and column types for the trade feed.

Binance WS trade payload: e event type, E event time (ms), s symbol, t trade id, p price (string),
q quantity (string), T trade time (ms), m is buyer maker, M ignore.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
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

PARSED_COL = "parsed"
REASON_COL = "reason"
