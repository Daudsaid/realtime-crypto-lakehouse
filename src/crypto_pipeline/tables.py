"""Table addressing and streaming I/O helpers.

The single place that knows whether a table is a Delta PATH (local, use_tables=false) or a
Unity Catalog table name (Databricks, use_tables=true). Jobs never build paths themselves.
"""

from __future__ import annotations

import logging
import os
import signal
import threading

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery

from crypto_pipeline.config import PipelineConfig

log = logging.getLogger(__name__)

SHUTDOWN_GRACE_SECONDS = 30


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


def _arm_shutdown_watchdog() -> None:
    """Hard-exit if a graceful shutdown stalls, so a stuck JVM call can never hang the job."""

    def _kill() -> None:
        log.error("Graceful shutdown exceeded %ss; forcing exit", SHUTDOWN_GRACE_SECONDS)
        os._exit(1)

    timer = threading.Timer(SHUTDOWN_GRACE_SECONDS, _kill)
    timer.daemon = True
    timer.start()


def await_queries(queries: list[StreamingQuery], poll_seconds: int = 5) -> None:
    """Block until a query ends or SIGINT/SIGTERM arrives, logging each new micro-batch.

    The signal handlers only set a flag (raising inside a py4j call corrupts the gateway); all
    queries are then stopped gracefully. The first query failure is re-raised.
    """
    stop_requested = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop_requested.set())

    last_batch: dict[str, int] = {}
    while not stop_requested.wait(poll_seconds) and all(q.isActive for q in queries):
        for query in queries:
            progress = query.lastProgress
            if progress and progress["batchId"] != last_batch.get(query.name):
                last_batch[query.name] = progress["batchId"]
                log.info(
                    "%s batch=%s rows=%s rate=%.1f rows/s",
                    query.name,
                    progress["batchId"],
                    progress["numInputRows"],
                    progress.get("processedRowsPerSecond") or 0.0,
                )
    if stop_requested.is_set():
        log.info("Stopping %s", ", ".join(q.name for q in queries))
        _arm_shutdown_watchdog()
    for query in queries:
        query.stop()
    for query in queries:
        if query.exception():
            raise query.exception()
