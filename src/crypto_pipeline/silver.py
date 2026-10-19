"""Silver job: bronze.trades_raw -> silver.trades (+ silver.trades_quarantine).

Rows are parsed with the explicit Binance schema and validated against the data contract.
Valid rows are deduplicated on (symbol, trade_id) within a watermark and written to silver.trades;
violating rows go to silver.trades_quarantine with the failing rule as `reason`.

Run: python -m crypto_pipeline.silver
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from crypto_pipeline.config import PipelineConfig, load_config
from crypto_pipeline.contract import Contract, load_contract, reason_column
from crypto_pipeline.schemas import (
    BINANCE_TRADE_SCHEMA,
    PARSED_COL,
    PRICE_TYPE,
    QTY_TYPE,
    QUOTE_QTY_TYPE,
    REASON_COL,
)
from crypto_pipeline.spark import get_spark
from crypto_pipeline.tables import await_queries, read_stream, start_stream

LAYER, TABLE, QUARANTINE_TABLE = "silver", "trades", "trades_quarantine"
DEDUPE_KEYS = ["symbol", "trade_id"]
DEDUPE_WATERMARK = "10 minutes"
log = logging.getLogger(__name__)


def parse_trades(bronze: DataFrame) -> DataFrame:
    """Parse the JSON payload and add typed columns; malformed JSON leaves them all NULL."""
    parsed = F.col(PARSED_COL)
    return (
        bronze.withColumn(PARSED_COL, F.from_json("payload", BINANCE_TRADE_SCHEMA))
        .withColumn("symbol", parsed["s"])
        .withColumn("trade_id", parsed["t"])
        .withColumn("price", parsed["p"].cast(PRICE_TYPE))
        .withColumn("qty", parsed["q"].cast(QTY_TYPE))
        .withColumn("trade_time", F.timestamp_millis(parsed["T"]))
        .withColumn("event_time", F.timestamp_millis(parsed["E"]))
        .withColumn("is_buyer_maker", parsed["m"])
    )


def classify(bronze: DataFrame, contract: Contract) -> DataFrame:
    """Parse bronze rows and add `reason`: the first violated contract rule, NULL when valid."""
    return parse_trades(bronze).withColumn(REASON_COL, reason_column(contract))


def to_silver(valid: DataFrame) -> DataFrame:
    """Project contract-valid rows to the silver.trades columns and add quote_qty."""
    return valid.select(
        "symbol",
        "trade_id",
        "price",
        "qty",
        (F.col("price") * F.col("qty")).cast(QUOTE_QTY_TYPE).alias("quote_qty"),
        "trade_time",
        "event_time",
        "is_buyer_maker",
        "kafka_partition",
        "kafka_offset",
        "ingest_ts",
    )


def dedupe_trades(trades: DataFrame) -> DataFrame:
    """Drop repeated (symbol, trade_id) pairs.

    Streaming input uses a trade_time watermark so dedupe state stays bounded; batch input
    (tests, backfills) has no state to bound, so a plain dropDuplicates on the same keys is used.
    """
    if not trades.isStreaming:
        return trades.dropDuplicates(DEDUPE_KEYS)
    return trades.withWatermark("trade_time", DEDUPE_WATERMARK).dropDuplicatesWithinWatermark(DEDUPE_KEYS)


def to_quarantine(invalid: DataFrame) -> DataFrame:
    """Project contract-violating rows to the quarantine layout."""
    return invalid.select(
        "payload",
        REASON_COL,
        "kafka_key",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        F.current_timestamp().alias("quarantined_at"),
    )


def run(cfg: PipelineConfig | None = None) -> None:
    """Start the silver and quarantine streams and block until one terminates."""
    cfg = cfg or load_config()
    spark = get_spark("crypto-silver", cfg)
    contract = load_contract(cfg.contract_path)
    log.info("Loaded contract %s v%s", contract.id, contract.version)

    classified = classify(read_stream(spark, cfg, "bronze", "trades_raw"), contract)
    valid = dedupe_trades(to_silver(classified.filter(F.col(REASON_COL).isNull())))
    invalid = to_quarantine(classified.filter(F.col(REASON_COL).isNotNull()))

    queries = [
        start_stream(valid, cfg, LAYER, TABLE),
        start_stream(invalid, cfg, LAYER, QUARANTINE_TABLE),
    ]
    await_queries(queries)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run()
