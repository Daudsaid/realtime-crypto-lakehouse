"""Bronze job: raw Kafka trades -> bronze.trades_raw (append-only, no parsing).

Run: python -m crypto_pipeline.bronze
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from crypto_pipeline.config import PipelineConfig, load_config
from crypto_pipeline.spark import get_spark
from crypto_pipeline.tables import await_queries, start_stream

LAYER, TABLE = "bronze", "trades_raw"
log = logging.getLogger(__name__)


def read_kafka(spark: SparkSession, cfg: PipelineConfig) -> DataFrame:
    """Streaming DataFrame over the raw trades topic."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka.bootstrap_servers)
        .option("subscribe", cfg.kafka.topic)
        .option("startingOffsets", cfg.kafka.starting_offsets)
        .option("maxOffsetsPerTrigger", cfg.kafka.max_offsets_per_trigger)
        .option("failOnDataLoss", "false")
        .load()
    )


def to_bronze(kafka_df: DataFrame) -> DataFrame:
    """Map the Kafka source schema to bronze columns; payload stays an unparsed JSON string."""
    return kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("payload"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingest_ts"),
    ).withColumn("ingest_date", F.to_date("ingest_ts"))


def start(spark: SparkSession, cfg: PipelineConfig) -> list[StreamingQuery]:
    """Start the bronze stream on an existing session."""
    query = start_stream(to_bronze(read_kafka(spark, cfg)), cfg, LAYER, TABLE, partition_by=["ingest_date"])
    log.info("Started %s -> %s.%s", cfg.kafka.topic, LAYER, TABLE)
    return [query]


def run(cfg: PipelineConfig | None = None) -> None:
    """Run bronze standalone and block until it terminates."""
    cfg = cfg or load_config()
    await_queries(start(get_spark("crypto-bronze", cfg), cfg))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run()
