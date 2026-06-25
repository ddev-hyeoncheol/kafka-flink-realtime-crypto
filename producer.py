import asyncio
import websockets
import json
from confluent_kafka import Producer

kafka_conf = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "binance-producer",
    "acks": "all",
    "linger.ms": 10,
}

producer = Producer(kafka_conf)


def delivery_report(err, msg):
    """Handle delivery reports from the Kafka broker."""
    if err is not None:
        print(f"Message delivery failed: {err}")


def normalize_trade(event: dict) -> dict:
    """Normalize raw Binance trade event payload into a unified format."""
    price_raw = event.get("p")
    quantity_raw = event.get("q")

    price = float(price_raw) if price_raw is not None else 0.0
    quantity = float(quantity_raw) if quantity_raw is not None else 0.0

    return {
        "event_time": event.get("E"),
        "symbol": event.get("s"),
        "trade_id": event.get("t"),
        "price": price,
        "quantity": quantity,
        "trade_time": event.get("T"),
        "is_buyer_maker": event.get("m"),
    }


async def connect_binance():
    """Connect to Binance WebSocket API and stream trade events to Kafka."""
    uri = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    async with websockets.connect(uri) as websocket:
        print("Successfully connected to Binance BTCUSDT stream.")

        while True:
            try:
                data = await websocket.recv()

                if isinstance(data, bytes):
                    data = data.decode("utf-8")

                event = json.loads(data)

                if event.get("e") == "trade":
                    normalized_data = normalize_trade(event)
                    symbol = normalized_data["symbol"]
                    value_json = json.dumps(normalized_data)

                    producer.produce(
                        topic="crypto-trades",
                        key=symbol.encode("utf-8"),
                        value=value_json.encode("utf-8"),
                        callback=delivery_report,
                    )

                producer.poll(0)

            except websockets.ConnectionClosed as e:
                print(f"WebSocket connection closed: {e}. Reconnecting...")
                break
            except Exception as e:
                print(f"Error during message reception: {e}. Reconnecting...")
                break


async def main():
    """Run the main loop and handle client reconnection."""
    while True:
        try:
            await connect_binance()
        except Exception as e:
            print(f"Failed to connect or maintain connection: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram terminated by user. Exiting...")
    finally:
        print("Flushing remaining messages in Kafka producer queue...")
        producer.flush(timeout=3.0)
