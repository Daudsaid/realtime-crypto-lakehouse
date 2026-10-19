"""SparkSession factory.

Locally builds a Delta-enabled session with the Kafka and Postgres connectors pulled via Ivy.
On Databricks the runtime already provides a configured session, so we just attach to it.
"""

from __future__ import annotations

import pyspark
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from crypto_pipeline.config import LAYERS, PipelineConfig

EXTRA_PACKAGES = [
    f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}",
    "org.postgresql:postgresql:42.7.4",
]


def _local_session(app_name: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
    )
    spark = configure_spark_with_delta_pip(builder, extra_packages=EXTRA_PACKAGES).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_spark(app_name: str, cfg: PipelineConfig) -> SparkSession:
    """Return a SparkSession for the configured platform; set up UC schemas when using tables."""
    if cfg.platform == "local":
        spark = _local_session(app_name)
    else:
        spark = SparkSession.builder.getOrCreate()

    if cfg.use_tables:
        if cfg.catalog:
            spark.sql(f"USE CATALOG {cfg.catalog}")
        for layer in LAYERS:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {layer}")
    return spark
