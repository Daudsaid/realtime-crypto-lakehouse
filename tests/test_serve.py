"""Unit tests for the Postgres serving logic: change-feed reduction, DDL and upsert SQL."""

from __future__ import annotations

from pyspark.sql import SparkSession

from crypto_pipeline.schemas import OHLC_SCHEMA, VOLATILITY_SCHEMA
from crypto_pipeline.serve import ddl_statements, latest_changes, pg_type, upsert_sql

KEYS = ["symbol", "minute"]
CHANGES_SCHEMA = (
    "symbol string, minute string, close double, _change_type string, "
    "_commit_version long, _commit_timestamp timestamp"
)


def test_latest_changes_keeps_newest_postimage_per_key(spark: SparkSession) -> None:
    rows = [
        ("BTCUSDT", "12:00", 1.0, "insert", 1, None),
        ("BTCUSDT", "12:00", 1.0, "update_preimage", 2, None),
        ("BTCUSDT", "12:00", 2.0, "update_postimage", 2, None),
        ("BTCUSDT", "12:00", 3.0, "update_postimage", 3, None),
        ("ETHUSDT", "12:00", 9.0, "insert", 1, None),
    ]
    out = latest_changes(spark.createDataFrame(rows, CHANGES_SCHEMA), KEYS)

    assert out.columns == ["symbol", "minute", "close"]
    assert {(r.symbol, r.close) for r in out.collect()} == {("BTCUSDT", 3.0), ("ETHUSDT", 9.0)}


def test_latest_changes_ignores_deletes(spark: SparkSession) -> None:
    rows = [("BTCUSDT", "12:00", 1.0, "delete", 2, None)]
    assert latest_changes(spark.createDataFrame(rows, CHANGES_SCHEMA), KEYS).count() == 0


def test_ddl_declares_primary_key_and_postgres_types() -> None:
    serving = ddl_statements("ohlc_1m", OHLC_SCHEMA, KEYS)[2]
    assert "PRIMARY KEY (symbol, minute)" in serving
    assert "minute TIMESTAMP" in serving
    assert "open NUMERIC(18,8)" in serving
    assert "trade_count BIGINT" in serving
    assert "price_stddev DOUBLE PRECISION" in ddl_statements("volatility_1m", VOLATILITY_SCHEMA, KEYS)[2]


def test_pg_type_maps_decimal_precision() -> None:
    assert pg_type(OHLC_SCHEMA["quote_volume"].dataType) == "NUMERIC(38,8)"


def test_upsert_sql_updates_only_non_key_columns() -> None:
    sql = upsert_sql("ohlc_1m", ["symbol", "minute", "close"], KEYS)
    assert sql.endswith("ON CONFLICT (symbol, minute) DO UPDATE SET close = EXCLUDED.close")
    assert "FROM staging.ohlc_1m" in sql
