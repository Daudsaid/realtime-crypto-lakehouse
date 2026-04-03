import asyncio
import json
import os
import websockets
from kafka import KafkaProducer

BINANCE_WS_URL = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/ethusdt@trade"
KAFKA_TOPIC = "crypto-ticks"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

async def stream():
    async with websockets.connect(BINANCE_WS_URL) as ws:
        print("Connected to Binance WebSocket")
        while True:
            message = await ws.recv()
            data = json.loads(message)
            tick = data["data"]
            payload = {
                "symbol": tick["s"],
                "price": float(tick["p"]),
                "quantity": float(tick["q"]),
                "trade_time": tick["T"]
            }
            producer.send(KAFKA_TOPIC, payload)
            print(f"Sent: {payload['symbol']} @ {payload['price']}")

if __name__ == "__main__":
    asyncio.run(stream())
