"""Table addressing and streaming I/O helpers.

The single place that knows whether a table is a Delta PATH (local, use_tables=false) or a
Unity Catalog table name (Databricks, use_tables=true). Jobs never build paths themselves.
"""

from __future__ import annotations

import logging
import os
import signal
import threading
import time
from collections.abc import Callable

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import DataStreamWriter, StreamingQuery
from pyspark.sql.types import StructType

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


def read_changes(spark: SparkSession, cfg: PipelineConfig, layer: str, table: str) -> DataFrame:
    """Stream the Delta Change Data Feed: an initial snapshot as inserts, then row-level changes.

    Adds _change_type, _commit_version and _commit_timestamp; the table must have the feed enabled.
    """
    reader = spark.readStream.format("delta").option("readChangeFeed", "true")
    location = table_location(cfg, layer, table)
    if cfg.use_tables:
        return reader.table(location)
    return reader.load(location)


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


def start_foreach_batch(
    df: DataFrame,
    cfg: PipelineConfig,
    layer: str,
    name: str,
    batch_fn: Callable[[DataFrame, int], None],
    output_mode: str = "update",
) -> StreamingQuery:
    """Start a foreachBatch query with its own checkpoint `<layer>_<name>` and the configured trigger."""
    writer = (
        df.writeStream.outputMode(output_mode)
        .queryName(f"{layer}_{name}")
        .option("checkpointLocation", checkpoint_path(cfg, layer, name))
        .foreachBatch(batch_fn)
    )
    return with_trigger(writer, cfg).start()


def _delta_table(spark: SparkSession, cfg: PipelineConfig, layer: str, table: str) -> DeltaTable:
    location = table_location(cfg, layer, table)
    return DeltaTable.forName(spark, location) if cfg.use_tables else DeltaTable.forPath(spark, location)


def ensure_table(
    spark: SparkSession,
    cfg: PipelineConfig,
    layer: str,
    table: str,
    schema: StructType,
    change_feed: bool = False,
) -> None:
    """Create the Delta table (path or Unity Catalog) if missing; optionally enable its change feed."""
    builder = DeltaTable.createIfNotExists(spark).addColumns(schema)
    location = table_location(cfg, layer, table)
    builder = builder.tableName(location) if cfg.use_tables else builder.location(location)
    builder.execute()

    if change_feed:
        properties = _delta_table(spark, cfg, layer, table).detail().first()["properties"]
        if properties.get("delta.enableChangeDataFeed") != "true":
            ident = location if cfg.use_tables else f"delta.`{location}`"
            spark.sql(f"ALTER TABLE {ident} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")


def upsert(
    spark: SparkSession, cfg: PipelineConfig, layer: str, table: str, updates: DataFrame, keys: list[str]
) -> None:
    """MERGE `updates` into the table on `keys`: matched rows are replaced, new keys inserted."""
    condition = " AND ".join(f"t.{key} = s.{key}" for key in keys)
    (
        _delta_table(spark, cfg, layer, table)
        .alias("t")
        .merge(updates.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


class _StopRequest:
    """Lock-free stop flag: a signal handler may run at any point, even inside another handler.

    Handlers must not take locks (threading.Event.set() deadlocks if a second signal arrives
    while the first handler is inside it, which `timeout` provokes by signalling the whole
    process group). A single attribute assignment is safe.
    """

    requested_at: float | None = None

    def __call__(self, signum: int, frame: object) -> None:
        if self.requested_at is None:
            self.requested_at = time.monotonic()

    def sleep(self, seconds: float) -> bool:
        """Sleep up to `seconds` in short slices; return True as soon as a stop is requested."""
        deadline = time.monotonic() + seconds
        while self.requested_at is None and time.monotonic() < deadline:
            time.sleep(0.2)
        return self.requested_at is not None


SHUTDOWN_NOTE = "expected during shutdown: the in-flight batch is cancelled and replays from its checkpoint"
# JVM loggers that report the cancelled batch as ERROR when queries are stopped mid-write.
JVM_SHUTDOWN_LOGGERS = (
    "org.apache.spark.sql.delta.files.DeltaFileFormatWriter",
    "org.apache.spark.storage.ShuffleBlockFetcherIterator",
    "org.apache.spark.broadcast.TorrentBroadcast",
    "org.apache.spark.util.Utils",
    "org.apache.spark.executor.Executor",
    "org.apache.spark.rpc.netty.Inbox",
)


class ExpectedShutdownErrors(logging.Filter):
    """Downgrade py4j callback ERRORs to WARNING once a shutdown was requested.

    Stopping a query cancels its in-flight foreachBatch (e.g. a Delta MERGE), which py4j reports as
    an ERROR with a traceback. That is expected, and safe because the batch replays on restart.
    """

    def __init__(self, stop: _StopRequest) -> None:
        super().__init__()
        self.stop = stop

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno >= logging.ERROR and self.stop.requested_at is not None:
            record.msg = f"{record.getMessage()} ({SHUTDOWN_NOTE})"
            record.args = ()
            record.levelno, record.levelname = logging.WARNING, "WARNING"
            record.exc_info = record.exc_text = None
        return True


def _silence_jvm_shutdown_errors() -> None:
    """Turn off the JVM loggers that report the cancelled in-flight batch as ERROR."""
    spark = SparkSession.getActiveSession()
    if spark is None:
        return
    jvm = spark.sparkContext._jvm
    configurator = jvm.org.apache.logging.log4j.core.config.Configurator
    for name in JVM_SHUTDOWN_LOGGERS:
        configurator.setLevel(name, jvm.org.apache.logging.log4j.Level.OFF)


def _start_shutdown_watchdog(stop: _StopRequest) -> None:
    """Hard-exit if a requested shutdown takes longer than the grace period.

    Runs in its own daemon thread from startup, so it works even if the main thread is stuck
    in a JVM call. Uses os.write (no logging locks) before os._exit.
    """

    def _watch() -> None:
        while True:
            time.sleep(0.5)
            if (
                stop.requested_at is not None
                and time.monotonic() - stop.requested_at > SHUTDOWN_GRACE_SECONDS
            ):
                os.write(2, f"Graceful shutdown exceeded {SHUTDOWN_GRACE_SECONDS}s; forcing exit\n".encode())
                os._exit(1)

    threading.Thread(target=_watch, name="shutdown-watchdog", daemon=True).start()


def await_queries(queries: list[StreamingQuery], poll_seconds: int = 5) -> None:
    """Block until a query ends or SIGINT/SIGTERM arrives, logging each new micro-batch.

    The signal handlers only set a lock-free flag (raising inside a py4j call corrupts the
    gateway); all queries are then stopped gracefully, with a hard-exit watchdog as a backstop.
    The first query failure is re-raised.
    """
    stop = _StopRequest()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, stop)
    _start_shutdown_watchdog(stop)
    logging.getLogger("py4j.clientserver").addFilter(ExpectedShutdownErrors(stop))

    last_batch: dict[str, int] = {}
    while not stop.sleep(poll_seconds) and all(q.isActive for q in queries):
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
    if stop.requested_at is not None:
        log.warning("Stopping %s; %s", ", ".join(q.name for q in queries), SHUTDOWN_NOTE)
        _silence_jvm_shutdown_errors()
    for query in queries:
        query.stop()
    for query in queries:
        if query.exception():
            raise query.exception()
