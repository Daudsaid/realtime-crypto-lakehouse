# CLAUDE.md: crypto-stream-pipeline

## Purpose
Portfolio project for London data engineering roles (fintech, banks, trading firms, Databricks shops).
A real-time crypto market-data lakehouse:
live Binance trades -> Kafka -> Spark Structured Streaming -> Delta Lake medallion (bronze/silver/gold)
-> Postgres serving layer -> Metabase dashboard.
Runs fully in Docker locally. Must later deploy to Databricks (Unity Catalog) with CONFIG changes only, no rewrites.
Hiring managers will read this repo: clean code, tests, clear commits and README matter as much as features.

## Current state (step 1 DONE and verified)
- docker-compose.yml services: kafka, kafka-init, postgres, metabase, spark, producer. All healthy.
- Producer streams btcusdt@trade + ethusdt@trade into Kafka topic `crypto.trades.raw` (3 partitions, key = symbol).
  Verified ~28k messages. All land in partition 0 because 2 keys hash there: expected, keep it, document it.
- scripts/healthcheck.sh may exist; Binance REST + Metabase /api/health verified with curl.
- src/crypto_pipeline/*.py, conf/*.yml, tests/*.py, contracts/crypto_trades.yaml, databricks.yml, ci.yml are EMPTY stubs.

## Environment (fixed; do not change versions without asking)
- macOS + OrbStack (Docker). Nothing installed on the Mac; all Python/Spark runs inside the `spark` container.
- Spark container: python:3.11-slim-bookworm, OpenJDK 17, PySpark 3.5.9, delta-spark 3.x, Scala 2.12.
  Repo mounted: ./src ./conf ./tests ./data ./checkpoints -> /app/... ; ivy-cache volume for jars.
  Jars via delta.configure_spark_with_delta_pip(builder, extra_packages=[
    "org.apache.spark:spark-sql-kafka-0-10_2.12:<pyspark.__version__>", "org.postgresql:postgresql:42.7.4"]).
- Kafka apache/kafka:4.0.0 (KRaft). Inside Docker: kafka:29092. From Mac: localhost:9092.
- Postgres 16: db `crypto` with schema `gold`; db `metabase` for Metabase metadata. User/pass crypto/crypto via .env (gitignored).
  JDBC from spark: jdbc:postgresql://postgres:5432/crypto (env POSTGRES_JDBC_URL, POSTGRES_USER, POSTGRES_PASSWORD).
- Metabase localhost:3000. Spark UI localhost:4040.
- Spark container env: PIPELINE_ENV=local, KAFKA_BOOTSTRAP_SERVERS=kafka:29092, KAFKA_TOPIC=crypto.trades.raw.
- NEVER add container_name to compose. Another project (tfl-disruptions-lakehouse) uses names kafka/postgres
  and the same ports; it is stopped while working here. Never touch its containers.
- User shell is zsh: any commands you give the user must be paste-safe (no inline # comments).

## Run commands
- Start stack:   docker compose up -d --build
- Run a job:     docker compose exec spark python -m crypto_pipeline.<bronze|silver|gold|serve>
- Tests:         docker compose exec spark pytest -q
- Lint:          docker compose exec spark ruff check src tests

## Design decisions (follow these)
1. Portability switch: conf/local.yml has use_tables: false -> Delta PATHS under /app/data/lakehouse/<layer>/<table>.
   conf/databricks.yml has use_tables: true, catalog: crypto -> Unity Catalog tables <layer>.<table> after USE CATALOG.
   All reads/writes go through src/crypto_pipeline/tables.py helpers (read_table, read_stream, start_stream,
   checkpoint_path, with_trigger). No hardcoded paths in jobs.
2. config.py: frozen dataclass loaded from conf/<PIPELINE_ENV>.yml; env vars override (KAFKA_*, POSTGRES_*).
   PIPELINE_CONF_DIR env var overrides conf location (needed on Databricks).
3. spark.py get_spark(app_name, cfg): local = Delta-enabled session (local[*], UTC timezone, shuffle partitions 4,
   driver memory 2g, log level WARN); databricks = SparkSession.builder.getOrCreate(). Creates bronze/silver/gold
   schemas only when use_tables is true.
4. Each streaming job: one checkpoint per query at <checkpoint_root>/<layer>_<table>; trigger from config
   (processing_time "10 seconds" locally, available_now on Databricks); logs batch progress; re-raises query exceptions.
5. Each job module exposes pure DataFrame->DataFrame transform functions (unit-testable) plus run().

## Layer specs
BRONZE bronze.trades_raw: append-only, no parsing. Columns: kafka_key, payload (raw JSON string), kafka_topic,
  kafka_partition, kafka_offset, kafka_timestamp, ingest_ts, ingest_date (partition column).
  Kafka options: startingOffsets earliest, maxOffsetsPerTrigger from config, failOnDataLoss false.

SILVER silver.trades (streaming from bronze):
  Binance WS trade fields: e event type, E event time ms, s symbol, t trade id, p price (string), q qty (string),
  T trade time ms, m is_buyer_maker, M ignore.
  Output: symbol, trade_id bigint, price decimal(18,8), qty decimal(18,8), quote_qty = price*qty,
  trade_time timestamp, event_time timestamp, is_buyer_maker boolean, kafka_partition, kafka_offset, ingest_ts.
  Dedupe on (symbol, trade_id) with withWatermark("trade_time", "10 minutes") + dropDuplicatesWithinWatermark.
  Contract rules (contracts/crypto_trades.yaml, ODCS-style, version 1.0.0): required fields not null, price > 0,
  qty > 0, symbol in [BTCUSDT, ETHUSDT], trade_time not more than 5 min in the future, payload parses as JSON.
  Failing rows -> silver.trades_quarantine with payload, reason (string), kafka metadata, quarantined_at.
  Implement rules by reading the contract YAML (not hardcoded twice). Use foreachBatch if needed to write both tables.

GOLD (from silver):
  gold.ohlc_1m: symbol, minute (window start), open, high, low, close, volume (sum qty), quote_volume,
  trade_count, vwap. open/close = first/last by trade_time (use min_by/max_by). Watermark + append or update via MERGE.
  gold.volatility_1m: symbol, minute, price_range (high-low), price_stddev, return_pct ((close-open)/open), tick_count.

SERVE: copy gold tables to Postgres schema gold (JDBC, overwrite or upsert per micro-batch), for Metabase.

## Step 2 notes
- Implemented conf/local.yml, conf/databricks.yml, config.py, spark.py, tables.py, bronze.py. schemas.py stays empty until step 3.
- tables.py also exposes table_location and await_query (SIGINT/SIGTERM set a flag, then the query is stopped gracefully; raising inside py4j corrupts the gateway).
- Verified: 47,616 rows, offsets 0..47615 contiguous with no duplicates across a stop/restart (checkpoint resumed).

## Roadmap (one step at a time; commit after each)
2. [DONE] Bronze     -> verify row count grows; restart resumes from checkpoint without duplicates
3. Silver + contract  -> verify dedupe + quarantine (inject a bad message into Kafka to prove it)
4. Gold               -> verify candles: high>=low, open/close within [low, high], unique (symbol, minute)
5. Serve + Metabase   -> gold in Postgres; user builds dashboard; screenshots in docs/screenshots/
6. Tests + CI         -> pytest with small local DataFrames (silver parse/contract/dedupe, gold OHLC math),
                         ruff, .github/workflows/ci.yml (build spark image or use setup-java + pip, run ruff + pytest)
7. Databricks         -> databricks.yml bundle (Declarative Automation Bundles, formerly Databricks Asset Bundles):
                         jobs for bronze/silver/gold, dev/prod targets, conf/databricks.yml, checkpoints in a UC Volume.
                         dbt-databricks for gold is optional here.
8. README             -> architecture diagram (mermaid), stack table, how to run, design decisions,
                         data contract, screenshots, "what I'd add in production", interview talking points.

## Working rules
- Do ONE roadmap step per request. At the end: run it, verify it, show evidence (row counts, sample rows),
  then stop and summarise. Do not start the next step unprompted.
- Run everything inside the spark container. Fix errors yourself before reporting.
- Keep versions pinned as above. Type hints, docstrings on modules, small functions, no dead code.
- Suggest a commit message per step: "Step N: <summary>". Ask before git push.
- Ask before anything destructive (deleting data/, checkpoints/, volumes, docker system prune).
- Keep this file updated: tick off completed roadmap steps and note any design changes.

## Git rules (strict)
- NEVER run git commit, git push, or any git command that writes history. The user commits manually.
- At the end of each step, only suggest a commit message (no Co-Authored-By, no Claude/AI trailers).
