"""Gold job: silver.trades -> gold.ohlc_1m and gold.volatility_1m (1-minute candles per symbol).

One streaming aggregation (update mode, 2 minute watermark on trade_time) feeds a foreachBatch
that MERGEs both tables on (symbol, minute). Update mode re-emits the full candle whenever a late
trade lands inside the watermark, so the MERGE rewrites it idempotently.

Run: python -m crypto_pipeline.gold
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from crypto_pipeline.config import PipelineConfig, load_config
from crypto_pipeline.schemas import OHLC_SCHEMA, PRICE_TYPE, VOLATILITY_SCHEMA, VOLUME_TYPE
from crypto_pipeline.spark import get_spark
from crypto_pipeline.tables import (
    await_queries,
    ensure_table,
    read_stream,
    start_foreach_batch,
    upsert,
)

LAYER, QUERY_NAME = "gold", "candles_1m"
OHLC_TABLE, VOLATILITY_TABLE = "ohlc_1m", "volatility_1m"
KEYS = ["symbol", "minute"]
WATERMARK = "2 minutes"
log = logging.getLogger(__name__)


def aggregate_candles(trades: DataFrame) -> DataFrame:
    """Aggregate trades into 1-minute candles per symbol (batch or streaming).

    open/close are the first/last price ordered by (trade_time, trade_id), so trades sharing a
    millisecond resolve deterministically. price_stddev is the population standard deviation.
    """
    order = F.struct("trade_time", "trade_id")
    candles = trades.groupBy("symbol", F.window("trade_time", "1 minute").alias("w")).agg(
        F.min_by("price", order).alias("open"),
        F.max("price").alias("high"),
        F.min("price").alias("low"),
        F.max_by("price", order).alias("close"),
        F.sum("qty").alias("volume"),
        F.sum("quote_qty").alias("quote_volume"),
        F.count(F.lit(1)).alias("trade_count"),
        F.stddev_pop("price").alias("price_stddev"),
    )
    vwap = F.col("quote_volume").cast("double") / F.col("volume").cast("double")
    return candles.select(
        "symbol",
        F.col("w.start").alias("minute"),
        "open",
        "high",
        "low",
        "close",
        F.col("volume").cast(VOLUME_TYPE),
        "quote_volume",
        "trade_count",
        vwap.cast(PRICE_TYPE).alias("vwap"),
        "price_stddev",
    )


def to_ohlc(candles: DataFrame) -> DataFrame:
    """Project candles to the gold.ohlc_1m columns."""
    return candles.select(*OHLC_SCHEMA.fieldNames())


def to_volatility(candles: DataFrame) -> DataFrame:
    """Project candles to gold.volatility_1m: range, stddev, return as a fraction, tick count."""
    open_, close = F.col("open").cast("double"), F.col("close").cast("double")
    return candles.select(
        "symbol",
        "minute",
        (F.col("high") - F.col("low")).cast(PRICE_TYPE).alias("price_range"),
        "price_stddev",
        ((close - open_) / open_).alias("return_pct"),
        F.col("trade_count").alias("tick_count"),
    )


def run(cfg: PipelineConfig | None = None) -> None:
    """Create the gold tables if missing, then start the candle stream and block until it ends."""
    cfg = cfg or load_config()
    spark = get_spark("crypto-gold", cfg)
    ensure_table(spark, cfg, LAYER, OHLC_TABLE, OHLC_SCHEMA)
    ensure_table(spark, cfg, LAYER, VOLATILITY_TABLE, VOLATILITY_SCHEMA)

    def write_batch(batch: DataFrame, batch_id: int) -> None:
        # No batch.persist(): a cached plan breaks Delta MERGE attribute resolution.
        upsert(spark, cfg, LAYER, OHLC_TABLE, to_ohlc(batch), KEYS)
        upsert(spark, cfg, LAYER, VOLATILITY_TABLE, to_volatility(batch), KEYS)
        log.info("gold batch=%s merged into %s and %s", batch_id, OHLC_TABLE, VOLATILITY_TABLE)

    trades = read_stream(spark, cfg, "silver", "trades").withWatermark("trade_time", WATERMARK)
    query = start_foreach_batch(aggregate_candles(trades), cfg, LAYER, QUERY_NAME, write_batch)
    await_queries([query])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run()
