from quixstreams import Application
import json
import logging
import os
import redis

# Log to a file next to this script
logging.basicConfig(
    filename=os.path.join(os.path.dirname(__file__), "bluesky-consumer.log"),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("bluesky-consumer")

def main():
    app = Application(
        broker_address="127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092",
        loglevel="INFO",
        consumer_group="kafka-stream-counter",
        auto_offset_reset="earliest",
        consumer_extra_config={
            # Resolve localhost to 127.0.0.1 to avoid IPv6 issues
            "broker.address.family": "v4",
        }
    )

    # One Redis connection for the whole run, not one per message
    redis_client = redis.Redis(host="127.0.0.1", port=6379, db=0)
    redis_client.ping()  # fail fast, with a clear error, if Redis is down

    try:
        with app.get_consumer() as consumer:
            consumer.subscribe(["bluesky-events"])

            while True:
                msg = consumer.poll(1)

                if msg is None:
                    print("Waiting...")
                    continue
                if msg.error() is not None:
                    log.error("Kafka error: %s", msg.error())
                    continue

                try:
                    key = msg.key().decode("utf8")
                    value = json.loads(msg.value())
                    offset = msg.offset()

                    # .get() so a message without "commit" doesn't raise KeyError
                    operation = value.get("commit", {}).get("operation")

                    if operation is not None:
                        # incr() atomically adds 1 to the counter named after the operation
                        redis_client.incr(operation)
                        print(f"{offset} --> {operation}")
                    else:
                        print(f"{offset} {key} - No 'operation' field in message")
                except Exception:
                    # A bad message or Redis hiccup is logged, not fatal
                    log.exception("Failed to process offset %s", msg.offset())

                # Mark processed (even on failure) so one bad message can't block the stream
                consumer.store_offsets(msg)
    except KeyboardInterrupt:
        # On Ctrl+C, print the final counts
        for key in redis_client.keys():
            print(f"Type: {key.decode('utf8')} | Count: {redis_client.get(key).decode('utf8')}")
    finally:
        redis_client.close()

if __name__ == "__main__":
    main()
