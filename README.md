# crypto-stream-pipeline

A real-time streaming data pipeline that ingests live cryptocurrency price ticks from the Binance WebSocket API, processes them through Apache Kafka, stores them in PostgreSQL, and transforms them with dbt into OHLC candlestick and volatility models.

## Architecture
```
Binance WebSocket → Producer → Kafka → Consumer → PostgreSQL → dbt
```

## Stack

| Layer | Tool |
|---|---|
| Streaming source | Binance WebSocket (BTC/ETH live trades) |
| Message broker | Apache Kafka (KRaft mode, no Zookeeper) |
| Producer | Python (kafka-python, websockets) |
| Consumer | Python (batch inserts, kafka-python, psycopg2) |
| Storage | PostgreSQL |
| Transformation | dbt (staging, OHLC, volatility models) |
| Orchestration | Docker Compose |

## dbt Models

- `stg_crypto_ticks` — cleans and casts raw tick data
- `crypto_ohlc_1min` — 1-minute OHLC candlesticks per symbol
- `crypto_price_volatility` — rolling volatility, price range, and tick count per minute

## How to Run
```bash
docker compose up -d
```

All services start automatically:
- Kafka broker (KRaft mode)
- PostgreSQL
- Producer (Binance WebSocket → Kafka)
- Consumer (Kafka → PostgreSQL, batch of 100)

## Run dbt Models
```bash
source .venv/bin/activate
cd dbt/crypto_dbt
dbt run
```

## Sample Queries
```sql
-- Row count
SELECT COUNT(*) FROM crypto_ticks;

-- OHLC candlesticks
SELECT * FROM crypto_ohlc_1min ORDER BY minute DESC LIMIT 20;

-- Most volatile minutes
SELECT symbol, minute, price_range, tick_count
FROM crypto_price_volatility
ORDER BY price_range DESC LIMIT 10;
```

## Screenshots

### Docker — All services running
![Docker](screenshots/docker.png)

### OHLC Candlesticks (dbt mart)
![OHLC](screenshots/ohlc.png)

### Raw tick data (40k+ rows ingested)
![Ticks](screenshots/ticks.png)

## What I'd Add in Production

- Schema Registry (Avro) for message validation
- Airflow DAG to trigger dbt runs on a schedule
- AWS S3 + Glue for cloud storage layer
- Grafana dashboard on top of the volatility model
