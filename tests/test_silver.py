"""Unit tests for the silver transforms: contract classification and deduplication."""

from __future__ import annotations

import json
import time
from decimal import Decimal
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from crypto_pipeline.contract import load_contract
from crypto_pipeline.silver import classify, dedupe_trades, to_silver

CONTRACT = load_contract(Path(__file__).resolve().parents[1] / "contracts" / "crypto_trades.yaml")

BRONZE_SCHEMA = StructType(
    [
        StructField("kafka_key", StringType()),
        StructField("payload", StringType()),
        StructField("kafka_topic", StringType()),
        StructField("kafka_partition", LongType()),
        StructField("kafka_offset", LongType()),
        StructField("kafka_timestamp", TimestampType()),
        StructField("ingest_ts", TimestampType()),
    ]
)


def trade_payload(**overrides: object) -> str:
    """A valid Binance trade JSON string; override fields by their wire names (s, t, p, q, T...)."""
    now_ms = int(time.time() * 1000)
    trade = {"e": "trade", "E": now_ms, "s": "BTCUSDT", "t": 1, "p": "50000.10", "q": "0.5",
             "T": now_ms, "m": False, "M": True}  # fmt: skip
    return json.dumps({**trade, **overrides})


def bronze_df(spark: SparkSession, *payloads: str) -> DataFrame:
    rows = [("K", p, "crypto.trades.raw", 0, i, None, None) for i, p in enumerate(payloads)]
    return spark.createDataFrame(rows, BRONZE_SCHEMA).withColumn("ingest_ts", F.current_timestamp())


def reasons(spark: SparkSession, *payloads: str) -> list[str | None]:
    classified = classify(bronze_df(spark, *payloads), CONTRACT).orderBy("kafka_offset")
    return [row.reason for row in classified.select("reason").collect()]


def test_valid_trade_passes(spark: SparkSession) -> None:
    assert reasons(spark, trade_payload()) == [None]


def test_valid_trade_is_projected_with_quote_qty(spark: SparkSession) -> None:
    classified = classify(bronze_df(spark, trade_payload()), CONTRACT)
    row = to_silver(classified.filter(F.col("reason").isNull())).collect()[0]
    assert row.symbol == "BTCUSDT"
    assert row.price == Decimal("50000.10")
    assert row.quote_qty == Decimal("25000.05")


def test_negative_price_is_quarantined(spark: SparkSession) -> None:
    assert reasons(spark, trade_payload(p="-100.0")) == ["price_positive"]


def test_unknown_symbol_is_quarantined(spark: SparkSession) -> None:
    assert reasons(spark, trade_payload(s="DOGEUSDT")) == ["symbol_allowed"]


def test_malformed_json_is_quarantined(spark: SparkSession) -> None:
    assert reasons(spark, '{"e":"trade","s":"BTCUSDT","p":"') == ["payload_is_json"]


def test_dedupe_drops_repeated_symbol_trade_id(spark: SparkSession) -> None:
    payloads = [trade_payload(t=7), trade_payload(t=7), trade_payload(t=8), trade_payload(s="ETHUSDT", t=7)]
    classified = classify(bronze_df(spark, *payloads), CONTRACT)
    silver = to_silver(classified.filter(F.col("reason").isNull()))

    deduped = dedupe_trades(silver)

    assert deduped.count() == 3
    pairs = {(r.symbol, r.trade_id) for r in deduped.collect()}
    assert pairs == {("BTCUSDT", 7), ("BTCUSDT", 8), ("ETHUSDT", 7)}
