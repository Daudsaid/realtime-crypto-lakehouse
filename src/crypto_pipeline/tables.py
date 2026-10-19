"""Table addressing and streaming I/O helpers.

The single place that knows whether a table is a Delta PATH (local, use_tables=false) or a
Unity Catalog table name (Databricks, use_tables=true). Jobs never build paths themselves.
"""

from __future__ import annotations

import logging
import signal
import threading

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery

from crypto_pipeline.config import PipelineConfig

log = logging.getLogger(__name__)


def table_location(cfg: PipelineConfig, layer: str, table: str) -> str:
    """Return the UC table name `<layer>.<table>` or the Delta path for it."""
    if cfg.use_tables:
        return f"{layer}.{table}"
    return f"{cfg.lakehouse_root}/{layer}/{table}"


def checkpoint_path(cfg: PipelineConfig, layer: str, table: str) -> str:
    """One checkpoint per streaming query: `<checkpoint_root>/<layer>_<table>`."""
    return f"{cfg.checkpoint_root}/{layer}_{table}"


def read_table(spark: SparkSession, cfg: PipelineConfig, layer: str, table: str) -> DataFrame:
    """Batch-read a Delta table."""
    location = table_location(cfg, layer, table)
    if cfg.use_tables:
        return spark.read.table(location)
    return spark.read.format("delta").load(location)


def read_stream(spark: SparkSession, cfg: PipelineConfig, layer: str, table: str) -> DataFrame:
    """Stream-read a Delta table."""
    location = table_location(cfg, layer, table)
    if cfg.use_tables:
        return spark.readStream.table(location)
    return spark.readStream.format("delta").load(location)


def with_trigger(writer: DataStreamWriter, cfg: PipelineConfig) -> DataStreamWriter:
    """Apply the configured trigger (available_now takes precedence over processing_time)."""
    if cfg.trigger.available_now:
        return writer.trigger(availableNow=True)
    return writer.trigger(processingTime=cfg.trigger.processing_time)


def start_stream(
    df: DataFrame,
    cfg: PipelineConfig,
    layer: str,
    table: str,
    partition_by: list[str] | None = None,
    output_mode: str = "append",
) -> StreamingQuery:
    """Start a Delta streaming write to `<layer>.<table>` with its own checkpoint and trigger."""
    writer = (
        df.writeStream.format("delta")
        .outputMode(output_mode)
        .queryName(f"{layer}_{table}")
        .option("checkpointLocation", checkpoint_path(cfg, layer, table))
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer = with_trigger(writer, cfg)

    location = table_location(cfg, layer, table)
    if cfg.use_tables:
        return writer.toTable(location)
    return writer.start(location)


def await_query(query: StreamingQuery, poll_seconds: int = 5) -> None:
    """Block until the query ends, logging each new micro-batch.

    SIGINT/SIGTERM only set a flag (raising inside a py4j call corrupts the gateway), and the
    loop then stops the query gracefully. Query failures are re-raised.
    """
    stop_requested = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop_requested.set())

    last_batch = -1
    while not stop_requested.is_set() and not query.awaitTermination(poll_seconds):
        progress = query.lastProgress
        if progress and progress["batchId"] != last_batch:
            last_batch = progress["batchId"]
            log.info(
                "%s batch=%s rows=%s rate=%.1f rows/s",
                query.name,
                last_batch,
                progress["numInputRows"],
                progress.get("processedRowsPerSecond") or 0.0,
            )
    if stop_requested.is_set():
        log.info("Stopping %s", query.name)
        query.stop()
    if query.exception():
        raise query.exception()
