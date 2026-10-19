"""Serve job: gold Delta tables -> Postgres schema `gold` for Metabase (local only).

Each gold table's Change Data Feed is streamed into Postgres. Per micro-batch the latest version of
every changed (symbol, minute) is written to a staging table over JDBC, then upserted into the
serving table with INSERT ... ON CONFLICT DO UPDATE, so replays are idempotent. On Databricks the
gold tables are queried directly with Databricks SQL, so this job is skipped when use_tables is true.

Run: python -m crypto_pipeline.serve
"""

from __future__ import annotations

import logging
from urllib.parse import urlparse

import psycopg
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    DataType,
    DecimalType,
    DoubleType,
    LongType,
    StringType,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

from crypto_pipeline.config import PipelineConfig, load_config
from crypto_pipeline.gold import KEYS, OHLC_TABLE, VOLATILITY_TABLE
from crypto_pipeline.schemas import OHLC_SCHEMA, VOLATILITY_SCHEMA
from crypto_pipeline.spark import get_spark
from crypto_pipeline.tables import await_queries, read_changes, start_foreach_batch

LAYER = "serve"
SERVING_SCHEMA, STAGING_SCHEMA = "gold", "staging"
TABLES: dict[str, StructType] = {OHLC_TABLE: OHLC_SCHEMA, VOLATILITY_TABLE: VOLATILITY_SCHEMA}
CHANGE_TYPES = ("insert", "update_postimage")
log = logging.getLogger(__name__)


def latest_changes(changes: DataFrame, keys: list[str]) -> DataFrame:
    """Reduce a change-feed batch to the newest post-image of each key, without CDF metadata columns.

    Several commits can touch one key inside a batch, but an upsert may affect a row only once.
    Deletes are ignored: gold candles are never deleted.
    """
    newest = Window.partitionBy(*keys).orderBy(F.col("_commit_version").desc())
    return (
        changes.filter(F.col("_change_type").isin(*CHANGE_TYPES))
        .withColumn("_rn", F.row_number().over(newest))
        .filter("_rn = 1")
        .drop("_rn", "_change_type", "_commit_version", "_commit_timestamp")
    )


def pg_type(dtype: DataType) -> str:
    """Postgres column type for a Spark type used in the gold schemas."""
    if isinstance(dtype, DecimalType):
        return f"NUMERIC({dtype.precision},{dtype.scale})"
    mapping = {
        StringType: "TEXT",
        TimestampType: "TIMESTAMP",
        LongType: "BIGINT",
        DoubleType: "DOUBLE PRECISION",
    }
    return mapping[type(dtype)]


def ddl_statements(table: str, schema: StructType, keys: list[str]) -> list[str]:
    """Idempotent DDL: serving table with PRIMARY KEY plus an unconstrained staging twin."""
    columns = ", ".join(f"{f.name} {pg_type(f.dataType)}" for f in schema.fields)
    return [
        f"CREATE SCHEMA IF NOT EXISTS {SERVING_SCHEMA}",
        f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA}",
        f"CREATE TABLE IF NOT EXISTS {SERVING_SCHEMA}.{table} ({columns}, PRIMARY KEY ({', '.join(keys)}))",
        f"CREATE TABLE IF NOT EXISTS {STAGING_SCHEMA}.{table} (LIKE {SERVING_SCHEMA}.{table})",
    ]


def upsert_sql(table: str, columns: list[str], keys: list[str]) -> str:
    """INSERT ... SELECT from staging with ON CONFLICT (keys) DO UPDATE for the non-key columns."""
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c not in keys)
    cols = ", ".join(columns)
    return (
        f"INSERT INTO {SERVING_SCHEMA}.{table} ({cols}) SELECT {cols} FROM {STAGING_SCHEMA}.{table} "
        f"ON CONFLICT ({', '.join(keys)}) DO UPDATE SET {updates}"
    )


def pg_connect(cfg: PipelineConfig) -> psycopg.Connection:
    """Open a psycopg connection from the JDBC URL and credentials in the config."""
    url = urlparse(cfg.postgres.jdbc_url.removeprefix("jdbc:"))
    return psycopg.connect(
        host=url.hostname,
        port=url.port,
        dbname=url.path.lstrip("/"),
        user=cfg.postgres.user,
        password=cfg.postgres.password,
    )


def create_tables(cfg: PipelineConfig) -> None:
    """Create the serving and staging tables in Postgres if missing."""
    with pg_connect(cfg) as conn:
        for table, schema in TABLES.items():
            for statement in ddl_statements(table, schema, KEYS):
                conn.execute(statement)


def write_staging(batch: DataFrame, cfg: PipelineConfig, table: str) -> None:
    """Replace the staging table contents with `batch` over JDBC (TRUNCATE, then insert)."""
    (
        batch.write.format("jdbc")
        .option("url", cfg.postgres.jdbc_url)
        .option("dbtable", f"{STAGING_SCHEMA}.{table}")
        .option("user", cfg.postgres.user)
        .option("password", cfg.postgres.password)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )


def make_batch_writer(cfg: PipelineConfig, table: str, schema: StructType):
    """foreachBatch function: stage the newest change per key, then upsert it into Postgres."""
    columns = schema.fieldNames()

    def write_batch(batch: DataFrame, batch_id: int) -> None:
        write_staging(latest_changes(batch, KEYS).select(*columns), cfg, table)
        with pg_connect(cfg) as conn:
            cursor = conn.execute(upsert_sql(table, columns, KEYS))
        log.info("serve %s batch=%s upserted %s rows", table, batch_id, cursor.rowcount)

    return write_batch


def start(spark: SparkSession, cfg: PipelineConfig) -> list[StreamingQuery]:
    """Start one change-feed stream per gold table; returns [] where serving is not needed."""
    if cfg.use_tables:
        log.info("use_tables is true: gold is queried directly (Databricks SQL); serve is a no-op")
        return []
    create_tables(cfg)
    return [
        start_foreach_batch(
            read_changes(spark, cfg, "gold", table),
            cfg,
            LAYER,
            table,
            make_batch_writer(cfg, table, schema),
            output_mode="append",
        )
        for table, schema in TABLES.items()
    ]


def run(cfg: PipelineConfig | None = None) -> None:
    """Run serve standalone; exits immediately when serving is not applicable."""
    cfg = cfg or load_config()
    queries = start(get_spark("crypto-serve", cfg), cfg)
    if queries:
        await_queries(queries)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run()
