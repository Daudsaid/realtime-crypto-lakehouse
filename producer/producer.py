"""Binance live trades -> Kafka. Raw events, keyed by symbol, with reconnect + clean shutdown."""
import asyncio
import json
import logging
import os
import signal

import websockets
from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "crypto.trades.raw")
SYMBOLS = [s.strip().lower() for s in os.getenv("SYMBOLS", "btcusdt,ethusdt").split(",") if s.strip()]
WS_BASE = os.getenv("BINANCE_WS_BASE", "wss://data-stream.binance.vision")

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("producer")


def build_url() -> str:
    streams = "/".join(f"{s}@trade" for s in SYMBOLS)
    return f"{WS_BASE}/stream?streams={streams}"


def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 50,
        "compression.type": "lz4",
        "client.id": "binance-trades-producer",
    })


def on_delivery(err, msg):
    if err is not None:
        log.error("delivery failed: %s", err)


def send(producer: Producer, key: bytes, value: bytes) -> None:
    while True:
        try:
            producer.produce(TOPIC, key=key, value=value, on_delivery=on_delivery)
            break
        except BufferError:
            producer.poll(0.5)
    producer.poll(0)


async def stream(producer: Producer) -> None:
    url = build_url()
    backoff = 1
    count = 0
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_queue=10_000) as ws:
                log.info("connected: %s", url)
                backoff = 1
                async for raw in ws:
                    msg = json.loads(raw)
                    data = msg.get("data")
                    if not data or "s" not in data:
                        continue
                    send(producer, key=data["s"].encode(), value=json.dumps(data, separators=(",", ":")).encode())
                    count += 1
                    if count % 1000 == 0:
                        log.info("produced %d trades", count)
            log.info("connection closed by server, reconnecting")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("websocket error: %s; retrying in %ss", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def main() -> None:
    producer = make_producer()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    task = asyncio.create_task(stream(producer))
    await stop.wait()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    log.info("shutting down, flushing kafka")
    producer.flush(10)


if __name__ == "__main__":
    asyncio.run(main())
