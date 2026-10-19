"""Run the whole lakehouse in ONE SparkSession: bronze, silver, gold and serve streams together.

A single session keeps memory use down versus one JVM per job. Shutdown (SIGINT/SIGTERM) is shared:
all queries are stopped gracefully, with the 30s hard-exit watchdog from tables.await_queries.
If any query fails the process exits non-zero so the container restart policy can recover it.

Run: python -m crypto_pipeline.run_all
"""

from __future__ import annotations

import logging

from crypto_pipeline import bronze, gold, serve, silver
from crypto_pipeline.config import PipelineConfig, load_config
from crypto_pipeline.spark import get_spark
from crypto_pipeline.tables import await_queries

log = logging.getLogger(__name__)


def run(cfg: PipelineConfig | None = None) -> None:
    """Start every layer on one session and block until shutdown or failure."""
    cfg = cfg or load_config()
    spark = get_spark("crypto-pipeline", cfg)
    queries = []
    for job in (bronze, silver, gold, serve):
        queries.extend(job.start(spark, cfg))
    log.info("Running %d streaming queries: %s", len(queries), ", ".join(q.name for q in queries))
    await_queries(queries)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run()
