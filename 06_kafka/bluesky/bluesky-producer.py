import websocket
import json
import os
import logging
from datetime import datetime
from quixstreams import Application
from quixstreams.models import TopicConfig

"""
This script demonstrates how to use a Kafka producer to stream real-time 
events from Bluesky Jetstream to a Kafka topic.

NOTE that it does not parse/consume messages, it publishes/produces them to Kafka.
"""

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")

# Log to a file next to this script (and to the console via the root handler default)
logging.basicConfig(
    filename=os.path.join(os.path.dirname(__file__), "bluesky-producer.log"),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("bluesky-producer")

app = Application(
    broker_address=KAFKA_BROKER,
    producer_extra_config={
        "broker.address.family": "v4",
        # Batch settings for better throughput
        "linger.ms": 5,  # Wait up to 5ms to batch messages
        "batch.size": 100000,  # Batch up to 100KB
    }
)
topic = app.topic(
    name="bluesky-events",
    value_serializer="json",
    config=TopicConfig(
        num_partitions=1, 
        replication_factor=3,
    )
)

# Create a kafka producer 
producer = app.get_producer()

message_count = 0
start_time = datetime.now()

def on_delivery(err, msg):
    """Kafka calls this once per message, after the broker accepts or rejects it."""
    if err is not None:
        log.error("Delivery failed for key=%s: %s", msg.key(), err)

def on_message(ws, message):
    global message_count
    try:
        data = json.loads(message)
        # Jetstream also sends "identity"/"account" events with no "commit"; skip them
        commit = data.get("commit")
        if commit is None:
            return
        serialized = topic.serialize(key=commit["rev"], value=data)

        # Reuse the same producer - no context manager!
        producer.produce(
            topic=topic.name,
            key=serialized.key,
            value=serialized.value,
            on_delivery=on_delivery,
        )
        message_count += 1
    except Exception:
        # One bad message should never kill the stream; log it with traceback and move on
        log.exception("Failed to process message: %.200s", message)
        return

    # Log rate every 100 messages
    if message_count % 100 == 0:
        elapsed = (datetime.now() - start_time).total_seconds()
        if elapsed > 0:
            log.info("Rate: %.2f msg/sec (total: %d)", message_count / elapsed, message_count)

def on_error(ws, error):
    """Called by websocket-client on connection errors."""
    log.error("WebSocket error: %s", error)

def on_close(ws, close_status_code, close_msg):
    log.info("WebSocket closed (%s %s), flushing remaining messages...", close_status_code, close_msg)
    producer.flush()

ws = websocket.WebSocketApp(
    "wss://jetstream2.us-west.bsky.network/subscribe?wantedCollections=app.bsky.feed.post",
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

try:
    ws.run_forever()
except KeyboardInterrupt:
    log.info("Interrupted by user")
finally:
    # Ensure we flush on exit
    producer.flush()
    log.info("Total messages produced: %d", message_count)
