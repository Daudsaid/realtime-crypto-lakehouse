# CLAUDE.md: realtime-crypto-lakehouse

## Purpose
Repo: https://github.com/Daudsaid/realtime-crypto-lakehouse (local folder is still crypto-stream-pipeline; compose project, container and volume names are unchanged on purpose).
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
- Run a job:     docker compose exec spark python -m crypto_pipeline.<bronze|silver|gold|serve>  (stop the pipeline service first: shared checkpoints)
- Live lakehouse: docker compose up -d  (service `pipeline` = python -m crypto_pipeline.run_all, all layers in one SparkSession)
- Metabase as code: python3 scripts/setup_metabase.py  (idempotent; needs MB_ADMIN_EMAIL/MB_ADMIN_PASSWORD in .env)
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

## Step 3 notes
- contracts/crypto_trades.yaml is the single source of rules; contract.py compiles `quality` rules to Spark expressions (reason = first violated rule, in contract order). Mounted into the spark container at /app/contracts; conf key `contract_path`.
- silver.py runs two streaming queries from bronze (silver_trades with dropDuplicatesWithinWatermark, silver_trades_quarantine), each with its own checkpoint. No foreachBatch needed.
- get_spark sets spark.sql.caseSensitive=true: Binance keys differ only by case (t/T, e/E, m/M).
- Malformed JSON detected via the `_corrupt_record` field in the from_json schema (PERMISSIVE otherwise returns a struct of NULLs).
- Verified: 150,404 bronze -> 150,400 silver + 3 quarantine (price_positive, symbol_allowed, payload_is_json); re-sent trade present twice in bronze, once in silver; 0 duplicate (symbol, trade_id).

## Step 4 notes
- gold.py: one streaming aggregation (update mode, 2 min watermark) in a single foreachBatch query that MERGEs gold.ohlc_1m and gold.volatility_1m on (symbol, minute). Checkpoint: checkpoints/gold_candles_1m (one query, two tables). Tables are created if missing via tables.ensure_table.
- open/close use min_by/max_by on struct(trade_time, trade_id) so same-millisecond trades resolve deterministically. price_stddev is population stddev; return_pct is the fraction (close-open)/open, not x100. vwap = quote_volume/volume computed in double, cast to decimal(18,8).
- Do not persist() the batch inside foreachBatch: the cached plan breaks Delta MERGE ("key not found: symbol#...").
- Shutdown: signal handlers must be lock-free. threading.Event.set() in a handler deadlocked when `timeout` delivered SIGINT twice (child + process group). tables._StopRequest is a plain flag; a daemon watchdog thread os._exit(1)s if shutdown exceeds 30s. spark service has `init: true` so orphaned JVMs are reaped instead of lingering as zombies.
- Bronze reads at most maxOffsetsPerTrigger=5000 per 10s, so after the pipeline has been off the lag takes a few minutes of bronze runtime to clear.
- Verified: 128..162 candles, 0 invariant violations, 0 duplicate (symbol, minute), full independent recompute from silver matches, 9/9 completed BTCUSDT minutes match Binance klines exactly.

## Step 5 notes
- Gold tables have Delta Change Data Feed on (ensure_table(change_feed=True), ALTER on existing tables). serve.py streams each table's CDF (initial snapshot first, so history backfills), keeps the newest post-image per (symbol, minute) per batch, writes it to Postgres staging.<table> via JDBC (truncate+overwrite), then INSERT ... ON CONFLICT (symbol, minute) DO UPDATE into gold.<table>. Staging lives in its own schema so Metabase (schema filter: gold) never sees it. DDL is derived from schemas.py.
- Serve is local-only: serve.start returns [] (no-op) when use_tables is true; on Databricks query gold with Databricks SQL.
- Every job module exposes start(spark, cfg) -> list[StreamingQuery] plus run(); run_all.py starts all four on one session. Compose: `spark` (idle, for exec) and `pipeline` share one image via an x-spark anchor; pipeline has restart: unless-stopped, init: true, stop_grace_period: 45s (watchdog fires at 30s).
- The pipeline container uses ~2.3 GiB of the ~3.9 GiB available to Docker; keep an eye on it before adding queries.
- Metabase: all five questions are native SQL (line/line/bar/line/table) over gold.ohlc_1m and gold.volatility_1m, last 3 hours relative to the newest candle; dashboard "Crypto Market - Live" in collection "Crypto Market". Admin credentials come from .env (gitignored).
- psycopg[binary] added to dependencies, py-spy to dev extras. max_offsets_per_trigger is now 50000 locally.

## Roadmap (one step at a time; commit after each)
2. [DONE] Bronze     -> verify row count grows; restart resumes from checkpoint without duplicates
3. [DONE] Silver + contract -> verify dedupe + quarantine (inject a bad message into Kafka to prove it)
4. [DONE] Gold       -> verify candles: high>=low, open/close within [low, high], unique (symbol, minute)
5. [DONE] Serve + Metabase -> gold in Postgres; user builds dashboard; screenshots in docs/screenshots/
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

## Author (use in README, step 8)
Daud Abdi, Associate Data Engineer, London, UK
Email: daudsaidabdi@gmail.com
GitHub: https://github.com/Daudsaid
LinkedIn: https://www.linkedin.com/in/daudabdi0506
Website: https://daudabdi.com
