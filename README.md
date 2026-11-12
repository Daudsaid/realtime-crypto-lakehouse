# realtime-crypto-lakehouse

Live Binance trades streamed through **Kafka** and **Spark Structured Streaming** into a **Delta Lake** medallion (bronze, silver, gold), served to **Postgres** and shown on a public **Streamlit** dashboard. A data contract decides what a valid trade is; every layer is idempotent and restart-safe. The same code is built to run on **Databricks** (Unity Catalog) by switching a config file.

**🔴 Live dashboard: [realtime-crypto-lakehouse.streamlit.app](https://realtime-crypto-lakehouse.streamlit.app/)**. Live BTC and ETH one-minute candles whenever the pipeline is running; the last three hours it has when it isn't.

![Crypto Market - Live dashboard](docs/dashboard.png)

## Contents

- [Results](#results)
- [Architecture](#architecture)
- [Tech stack](#tech-stack)
- [Run it locally](#run-it-locally)
- [Publish the dashboard](#publish-the-dashboard)
- [Data contract](#data-contract)
- [Design decisions](#design-decisions)
- [Engineering challenges solved](#engineering-challenges-solved)
- [Findings from the data](#findings-from-the-data)
- [Testing](#testing)
- [Repository structure](#repository-structure)
- [What I'd add in production](#what-id-add-in-production)
- [Interview talking points](#interview-talking-points)
- [Roadmap](#roadmap)

## Results

Measured on 19 Oct 2026 after stopping the pipeline and draining every layer: **762,583 trades** and **408 one-minute candles** (204 each for BTCUSDT and ETHUSDT).

| Claim | Evidence |
| --- | --- |
| **Matches the exchange** | 198 of 202 complete minutes per symbol equal Binance's official klines exactly on open, high, low, close, volume and trade count. The other 4 are boundary minutes of two producer outages (WebSocket keepalive timeouts and DNS failures), where Binance trade ids jump by 952,720 (BTC) and 1,227,273 (ETH). No ingested minute has an internal trade-id gap (0 of 408). |
| **Exactly-once ingestion** | Bronze holds 762,583 rows with 762,583 distinct Kafka offsets, contiguous from 0 to 762,582, across many stops and restarts. |
| **No duplicate trades** | Silver has 762,583 rows and 762,583 distinct `(symbol, trade_id)`. An injected replay was dropped: bronze 762,587 = silver 762,583 + quarantine 3 + 1 deduplicated. |
| **Contract-driven quarantine** | Three deliberately bad messages (negative price, unknown symbol, malformed JSON) were rejected as `price_positive`, `symbol_allowed` and `payload_is_json`. |
| **Lossless serving** | All 408 candles are identical in Delta and Postgres. An independent batch-SQL recompute from silver matches all 408, and every invariant holds (high ≥ low, open and close within the range, 0 duplicate `(symbol, minute)`). |
| **Clean shutdown** | `docker compose stop pipeline` stops all 6 streaming queries in 2 s, exit code 0, with no errors or tracebacks. |

## Architecture

```mermaid
flowchart LR
    WS["Binance WebSocket<br/>btcusdt, ethusdt @trade"] --> PR["Producer<br/>idempotent, acks=all"]
    PR --> KF[("Kafka 4.0 KRaft<br/>crypto.trades.raw")]

    subgraph SP["One SparkSession: crypto_pipeline.run_all"]
        KF --> BR["bronze.trades_raw<br/>raw JSON + Kafka metadata"]
        BR --> SI["silver.trades<br/>typed, validated, deduped"]
        BR --> QU["silver.trades_quarantine<br/>rejects + reason"]
        SI --> OH["gold.ohlc_1m"]
        SI --> VO["gold.volatility_1m"]
        OH -. "Change Data Feed" .-> SV["serve<br/>staging + upsert"]
        VO -. "Change Data Feed" .-> SV
    end

    CT[["contracts/crypto_trades.yaml"]] --> SI
    CT --> QU
    SV --> PG[("Postgres 16<br/>local or Neon")]
    PG --> ST["Streamlit dashboard<br/>Community Cloud"]
```

| Layer | Table | Grain | What it holds |
| --- | --- | --- | --- |
| Bronze | `bronze.trades_raw` | one row per Kafka message | Raw payload plus Kafka key, partition, offset and timestamp. Append-only, so everything downstream can be rebuilt. |
| Silver | `silver.trades` | one row per `(symbol, trade_id)` | Parsed with an explicit schema, `decimal(18,8)` prices, UTC timestamps, `quote_qty`, deduplicated. |
| Silver | `silver.trades_quarantine` | one row per rejected message | Original payload, the violated rule as `reason`, Kafka metadata, `quarantined_at`. |
| Gold | `gold.ohlc_1m` | one row per `(symbol, minute)` | Open, high, low, close, volume, quote volume, trade count, VWAP. |
| Gold | `gold.volatility_1m` | one row per `(symbol, minute)` | High-low range, price standard deviation, return, tick count. |
| Serving | Postgres `gold.*` | same as gold | Query layer for the dashboard, `PRIMARY KEY (symbol, minute)`. |

## Tech stack

| Layer | Tool | Why |
| --- | --- | --- |
| Ingest | Python 3.11, `websockets`, `confluent-kafka` | Idempotent producer (`acks=all`, lz4), keyed by symbol. |
| Broker | Apache Kafka 4.0 (KRaft) | Durable, replayable buffer between the exchange and processing; no ZooKeeper. |
| Processing | PySpark 3.5.9 Structured Streaming, Java 17 | Stateful streaming (watermarks, dedupe, windows); the same engine as Databricks. |
| Storage | Delta Lake 3.3 | ACID `MERGE`, Change Data Feed, time travel. |
| Serving | PostgreSQL 16 (Docker or [Neon](https://neon.com)), `psycopg` 3 | Fast dashboard queries; idempotent upserts. |
| Dashboard | Streamlit, Altair | Public live view, hosted on Streamlit Community Cloud. |
| Runtime | Docker Compose (OrbStack) | One command brings up the whole lakehouse. |
| Quality | pytest, ruff | 28 unit tests; linted and formatted. |

## Run it locally

You need Docker (or OrbStack) with about 4 GiB of memory.

```bash
git clone https://github.com/Daudsaid/realtime-crypto-lakehouse.git
cd realtime-crypto-lakehouse
cp .env.example .env
docker compose up -d --build
```

This starts Kafka, Postgres, the Binance producer and the `pipeline` service: bronze, silver, gold and serve as six streaming queries in one Spark session. The first start downloads the Spark connectors, so allow a few minutes, then check the stack:

```bash
bash scripts/healthcheck.sh
```

and look at the newest candles:

```bash
docker compose exec postgres psql -U crypto -d crypto -c "select symbol, minute, open, high, low, close from gold.ohlc_1m order by minute desc limit 6;"
```

| Service | Endpoint | Notes |
| --- | --- | --- |
| Postgres | localhost:5432 | Database `crypto`, schema `gold` (credentials in `.env`) |
| Kafka | localhost:9092 | Topic `crypto.trades.raw`; `kafka:29092` inside Docker |
| Spark UI | localhost:4040 | Available while a job runs in the `spark` container |
| `pipeline` | none | All four layers in one SparkSession |
| `spark` | none | Idle container for tests, lint and one-off jobs |

```bash
docker compose logs -f pipeline
docker compose exec spark pytest -q
docker compose exec spark ruff check src tests
docker compose stop pipeline
```

To run a single layer by hand, stop `pipeline` first, because both use the same checkpoints:

```bash
docker compose stop pipeline
docker compose exec spark python -m crypto_pipeline.silver
```

## Publish the dashboard

`dashboard/app.py` reads `gold.ohlc_1m` and `gold.volatility_1m`: latest prices, close price per symbol, traded value and one-minute volatility for the last three hours, refreshed every 30 seconds. If the newest candle is more than three minutes old, it shows the last data it has under an "offline" banner instead of an empty page.

Streamlit Community Cloud hosts only the dashboard. The pipeline keeps running in Docker and serves gold to a free hosted Postgres:

```text
Laptop (Docker): Binance → Kafka → Spark → Delta → serve ──► Neon Postgres ◄── Streamlit Community Cloud
```

1. **Create the database.** Create a [Neon](https://neon.com) project and copy its connection string from **Connect** (`postgresql://USER:PASSWORD@HOST.neon.tech/neondb?sslmode=require`). The direct endpoint suits this single long-running writer better than the `-pooler` one.
2. **Point the pipeline at it.** Set `DATABASE_URL` in `.env` and run `docker compose up -d`. Serve creates the tables in Neon. On a fresh lakehouse its Change Data Feed snapshot backfills all history; on an existing one it continues from its checkpoint, so copy the history across once:
   ```bash
   docker compose exec -T postgres pg_dump -U crypto -d crypto --schema=gold --data-only --inserts --on-conflict-do-nothing | docker compose exec -T postgres psql "$(sed -n 's/^DATABASE_URL=//p' .env | tr -d "\"'")" -q
   ```
   `bash scripts/healthcheck.sh` then counts the candles in Neon.
3. **Deploy.** At [share.streamlit.io](https://share.streamlit.io), create an app from this repository with main file `dashboard/app.py`, and add `DATABASE_URL = "..."` under **Advanced settings → Secrets** (template: `dashboard/.streamlit/secrets.toml.example`).

To run the dashboard on your machine against the Docker Postgres instead: `pip install -r dashboard/requirements.txt`, then `streamlit run dashboard/app.py`.

## Data contract

`contracts/crypto_trades.yaml` (ODCS-style, version 1.0.0) is the single definition of a valid trade. `crypto_pipeline/contract.py` reads its `required` flags and `quality` rules at runtime and compiles them to Spark expressions, so no rule is written twice.

```yaml
quality:
  - name: payload_is_json
    rule: valid_json
  - name: required_not_null
    rule: required_fields
  - name: price_positive
    rule: positive
    column: price
  - name: qty_positive
    rule: positive
    column: qty
  - name: symbol_allowed
    rule: allowed_values
    column: symbol
    values: [BTCUSDT, ETHUSDT]
  - name: trade_time_not_future
    rule: max_future_minutes
    column: trade_time
    minutes: 5
```

A row's `reason` is the first rule it violates, in contract order; NULL means valid. Valid rows are deduplicated into `silver.trades` and the rest go to `silver.trades_quarantine`. Possible reasons: `payload_is_json`, `required_not_null:<field>`, `price_positive`, `qty_positive`, `symbol_allowed`, `trade_time_not_future`.

## Design decisions

**Kafka and Spark do different jobs.** Kafka decouples ingestion from processing: it holds trades durably while Spark is slow, restarting or down, and allows replay. Spark does the stateful work (parsing, contract checks, deduplication, windowed candles) and the exactly-once writes to Delta. During one 13-minute pipeline outage, Kafka held every trade and Spark processed them all on recovery.

**Bronze is raw and append-only.** The payload is stored unparsed with its Kafka metadata, so silver and gold can be rebuilt, or re-validated against a new contract version, without going back to the exchange.

**One code path for local and Databricks.** `conf/local.yml` (`use_tables: false`) addresses Delta by path under `data/lakehouse/<layer>/<table>`; `conf/databricks.yml` (`use_tables: true`) addresses Unity Catalog tables `<layer>.<table>`. Every read, write and checkpoint goes through `tables.py`, so the jobs contain no paths. On Databricks gold is queried with Databricks SQL and serve is a no-op.

**Keyed by symbol.** Kafka keys are the symbol, which preserves per-symbol order. With two symbols both keys hash to one of the three partitions, an accepted trade-off at this scale.

**Deduplication with bounded state.** `withWatermark("trade_time", "10 minutes")` plus `dropDuplicatesWithinWatermark(["symbol", "trade_id"])` removes replays without state growing forever.

**Deterministic open and close.** Trades often share a millisecond, so `min_by`/`max_by` order by `struct(trade_time, trade_id)`. Binance trade ids are sequential, so the candle matches the exchange's own.

**Late trades update candles through `MERGE`.** Gold aggregates in update mode with a 2-minute watermark and upserts both tables from one `foreachBatch`. A late trade inside the watermark re-emits the whole candle and the `MERGE` rewrites it, so replays are idempotent.

**Change Data Feed to Postgres.** Serve keeps the newest post-image per `(symbol, minute)` in each batch, writes it to a staging table over JDBC, then runs `INSERT ... ON CONFLICT (symbol, minute) DO UPDATE`. Retries and restarts can't create duplicates. One `DATABASE_URL` setting switches the target from the Docker Postgres to Neon.

**One SparkSession locally.** `run_all` runs all six queries in a single JVM, which fits a laptop's Docker memory. Each query keeps its own checkpoint and each job keeps its own entrypoint.

**The dashboard never goes blank.** It shows the three hours up to the newest candle, not up to now, so visitors see the last known market whenever the pipeline is off.

## Engineering challenges solved

**Shutdown hang (signal-handler deadlock).** A job ignored `timeout -s INT` and never exited. `py-spy dump` showed the main thread blocked in `threading.Event.set()` inside my signal handler: `timeout` sends SIGINT twice (to the process and its group), and the second handler waited on the lock the first one held. Fix: the handler only assigns a timestamp (lock-free), the main loop polls it, and a watchdog thread calls `os._exit(1)` if shutdown takes over 30 s.

**Zombie JVMs.** Every job run left a `[java] <defunct>` process. The Spark JVM was orphaned when Python exited and reparented to PID 1, which was `sleep infinity` and never reaps children. Fix: `init: true` in Compose, so `tini` is PID 1.

**Malformed JSON looked like a missing symbol.** A truncated payload was quarantined as `required_not_null:symbol`. In PERMISSIVE mode `from_json` returns a struct of NULLs, not a NULL struct. Fix: a `_corrupt_record` field in the schema, treated as `payload_is_json`.

**Delta `MERGE` crash.** Gold's `foreachBatch` failed with `key not found: symbol#808`. `batch.persist()` put an `InMemoryRelation` in the plan that Delta's `MERGE` could not resolve. Fix: no caching inside `foreachBatch`.

**Crash loop on a fresh lakehouse.** On a brand-new machine the pipeline restarted forever with `DELTA_SCHEMA_NOT_SET`: silver started reading bronze before bronze had made its first commit, and gold had the same race with silver. Fix: `wait_for_table` in `tables.py` makes each downstream stream wait for its upstream table, and fails fast if the upstream query dies or nothing arrives within 10 minutes.

## Findings from the data

From the 19 Oct 2026 run (408 candles):

- **BTC and ETH move together.** The correlation of their one-minute returns is 0.915.
- **BTC dominates traded value.** BTCUSDT is 68.7% of all quote volume (133.7M of 194.7M USDT).
- **ETH is consistently more volatile.** Its average one-minute range is 0.071% against 0.047% for BTC, and wider than BTC's in 86.8% of minutes.
- **Volume and volatility spike together.** The correlation of quote volume and range is 0.62 for BTC and 0.54 for ETH.

## Testing

```bash
docker compose exec spark pytest -q
```

28 unit tests run on small in-memory DataFrames with a local Spark session; no Kafka or Delta needed.

| File | Tests | Covers |
| --- | --- | --- |
| `tests/test_silver.py` | 6 | Valid trades and `quote_qty`; negative price, unknown symbol and malformed JSON map to the right reason; deduplication. |
| `tests/test_gold.py` | 6 | OHLC with shuffled input and a same-millisecond tie, volume, VWAP, standard deviation, range, return, single-tick candles, per-symbol separation. |
| `tests/test_serve.py` | 5 | Newest-version selection from change-feed batches, delete handling, Postgres DDL and types, upsert SQL. |
| `tests/test_tables.py` | 6 | Lock-free stop flag, shutdown log handling, waiting for upstream tables on a fresh lakehouse. |
| `tests/test_config.py` | 5 | `DATABASE_URL` parsing (Neon URLs, encoded passwords, ports) and its precedence over `POSTGRES_*`. |

## Repository structure

```text
.
├── docker-compose.yml        Kafka, Postgres, producer, spark and pipeline services
├── conf/
│   ├── local.yml             Delta paths, Kafka and trigger settings for Docker
│   └── databricks.yml        Unity Catalog profile (use_tables: true)
├── contracts/
│   └── crypto_trades.yaml    Data contract enforced by silver
├── producer/                 Binance WebSocket to Kafka producer
├── spark/Dockerfile          Python 3.11, Java 17, PySpark, Delta
├── postgres/init/            Creates the gold schema
├── src/crypto_pipeline/
│   ├── config.py             Config from conf/<env>.yml plus env overrides (incl. DATABASE_URL)
│   ├── spark.py              SparkSession factory (local Delta or Databricks)
│   ├── tables.py             Path/UC addressing, streaming I/O, MERGE, startup and shutdown handling
│   ├── schemas.py            Binance and gold table schemas
│   ├── contract.py           Contract loader and rule compiler
│   ├── bronze.py             Kafka to bronze.trades_raw
│   ├── silver.py             Parse, validate, deduplicate, quarantine
│   ├── gold.py               One-minute OHLC and volatility candles
│   ├── serve.py              Change Data Feed to Postgres upserts
│   └── run_all.py            All four jobs in one SparkSession
├── dashboard/
│   ├── app.py                Streamlit dashboard over the gold tables
│   └── requirements.txt      Dashboard dependencies (Streamlit Community Cloud)
├── scripts/
│   └── healthcheck.sh        Stack smoke test
├── docs/                     README images
└── tests/                    pytest suite (28 tests)
```

## What I'd add in production

- **Always-on hosting.** Run the producer and pipeline on managed infrastructure (Confluent Cloud or MSK for Kafka, Databricks jobs for Spark) instead of a laptop, so the dashboard is always live.
- **Producer resilience.** Reconnect with backoff and backfill gaps from Binance's REST API after a WebSocket drop; the two outages in the results were found by hand.
- **Completeness alerting.** Alert on trade-id gaps per symbol, on Kafka consumer lag and on the age of the newest gold candle.
- **Table maintenance.** Scheduled `OPTIMIZE` and `VACUUM` for the small files a 10-second trigger produces.
- **CI/CD.** GitHub Actions running ruff and pytest on every push, and a Databricks bundle deploying jobs to dev and prod targets.
- **Schema evolution.** Contract versioning with a compatibility check before a new version reaches silver.
- **Secrets.** A secrets manager rather than `.env` files.

## Interview talking points

- **Why both Kafka and Spark?** Kafka buffers and replays; Spark does the stateful processing. Kafka held every trade through a 13-minute pipeline outage and Spark caught up on recovery without losing or duplicating any.
- **Exactly-once end to end.** Checkpointed Kafka offsets into bronze, watermarked deduplication in silver, `MERGE` in gold and `ON CONFLICT` upserts in Postgres. Each step can replay safely.
- **Correctness is measured, not assumed.** Candles reconcile with Binance's own klines, an independent batch recompute matches the streaming result, and invariants are checked.
- **Data contracts as code.** One YAML file defines validity; the pipeline compiles it, and rejects carry the rule they broke.
- **Debugging below the API.** The signal-handler deadlock (found with `py-spy`), zombie JVMs, a PERMISSIVE-mode JSON edge case, a Delta `MERGE` plan bug and a fresh-start race.
- **Portability.** One config switch moves the same jobs from local Delta paths to Unity Catalog.

## Roadmap

- [x] Kafka producer, KRaft broker and Postgres in Docker Compose
- [x] Bronze, silver (data contract, deduplication, quarantine) and gold (OHLC, volatility) streaming jobs
- [x] Idempotent Change Data Feed serving to Postgres
- [x] Single-session `pipeline` service with clean shutdown and safe fresh starts
- [x] Live Streamlit dashboard on Community Cloud, served from Neon
- [x] Unit tests and lint
- [ ] CI with GitHub Actions (`.github/workflows/ci.yml` is still empty)
- [ ] Databricks deployment with a bundle and Unity Catalog. The config switch and `conf/databricks.yml` exist; the bundle (`databricks.yml`) is not written and nothing has run on Databricks yet
- [ ] dbt-databricks for the gold layer
- [ ] More symbols, which also spreads load across Kafka partitions

## Author

**Daud Abdi**, Associate Data Engineer, London, UK

[GitHub](https://github.com/Daudsaid) · [LinkedIn](https://www.linkedin.com/in/daudabdi0506) · [daudabdi.com](https://daudabdi.com) · [daudsaidabdi@gmail.com](mailto:daudsaidabdi@gmail.com)
