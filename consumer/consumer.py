import json
import os
import time
import psycopg2
from kafka import KafkaConsumer

KAFKA_TOPIC = "crypto-ticks"
BATCH_SIZE = 100
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5433")
PG_DB = os.getenv("POSTGRES_DB", "crypto_stream_db")
PG_USER = os.getenv("POSTGRES_USER", "daudsaid")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "password")

# Wait for Postgres
while True:
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=int(PG_PORT),
            dbname=PG_DB,
            user=PG_USER,
            password=PG_PASS
        )
        break
    except Exception as e:
        print(f"Waiting for Postgres... {e}")
        time.sleep(3)

cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS crypto_ticks (
        id SERIAL PRIMARY KEY,
        symbol VARCHAR(20),
        price NUMERIC(18, 8),
        quantity NUMERIC(18, 8),
        trade_time BIGINT,
        ingested_at TIMESTAMP DEFAULT NOW()
    )
""")
conn.commit()

# Wait for Kafka
while True:
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            auto_offset_reset="latest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8"))
        )
        break
    except Exception as e:
        print(f"Waiting for Kafka... {e}")
        time.sleep(3)

print("Consumer started, waiting for messages...")

batch = []
for message in consumer:
    batch.append(message.value)
    if len(batch) >= BATCH_SIZE:
        cursor.executemany("""
            INSERT INTO crypto_ticks (symbol, price, quantity, trade_time)
            VALUES (%(symbol)s, %(price)s, %(quantity)s, %(trade_time)s)
        """, batch)
        conn.commit()
        print(f"Inserted batch of {len(batch)} records")
        batch = []
