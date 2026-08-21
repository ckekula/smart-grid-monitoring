import json
import logging
import sys

from confluent_kafka import KafkaException, Producer

from config import DATASET_PATH, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from data.loader import load_dataset
from data.simulator import simulate_events
from data.transformer import transform_dataset

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("smart-grid-producer")


def delivery_report(err, message):
    """
    Called by confluent-kafka after Kafka acknowledges a message.
    """

    if err is not None:
        logger.error(
            "Message delivery failed | topic=%s | error=%s",
            message.topic(),
            err,
        )
        return

    logger.debug(
        "Message delivered | topic=%s | partition=%s | offset=%s | key=%s",
        message.topic(),
        message.partition(),
        message.offset(),
        message.key().decode("utf-8") if message.key() else None,
    )

def create_producer() -> Producer:
    config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all", # Wait for the broker to acknowledge the message.
        "retries": 5, # Retry transient delivery failures.
        "enable.idempotence": True, # Keep messages ordered within a partition.

        # Small batching delay. The simulator controls the actual simulation rate,
        # so Kafka itself does not need aggressive batching.
        "linger.ms": 5,
    }

    logger.info("Creating Kafka producer | bootstrap_servers=%s", KAFKA_BOOTSTRAP_SERVERS)

    return Producer(config)


def produce_events(producer: Producer, events: list[dict]) -> None:
    """
    The simulator emits one timestamp group at a time. Each group contains
    the 20 meter readings for that simulated 15-minute interval.

    Kafka key: grid_zone
    Events from the same zone are routed consistently to the same Kafka partition.
    """

    total_events = len(events)
    produced_events = 0
    current_timestamp = None
    current_interval_count = 0

    logger.info(
        "Starting smart-meter simulation | total_events=%s",
        f"{total_events:,}",
    )

    try:
        for event in simulate_events(events):
            timestamp = event["timestamp"]

            if timestamp != current_timestamp:
                current_timestamp = timestamp
                current_interval_count = 0

                logger.info("Streaming interval | timestamp=%s", timestamp)

            current_interval_count += 1
            produced_events += 1

            key = event["grid_zone"]

            producer.produce(
                topic=KAFKA_TOPIC,
                key=key.encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
                callback=delivery_report,
            )

            producer.poll(0)

        logger.info("Waiting for pending Kafka messages to be delivered...")
        remaining = producer.flush()

        if remaining > 0:
            logger.error(
                "Failed to deliver %s messages",
                remaining,
            )
            raise RuntimeError(f"{remaining} Kafka messages were not delivered")

        logger.info(
            "Streaming completed successfully | events=%s",
            f"{produced_events:,}",
        )

    except BufferError:
        logger.warning("Kafka producer queue is full; waiting for delivery...")
        producer.poll(1)

    except KafkaException as exc:
        logger.exception("Kafka error while producing events: %s", exc)
        raise


def main() -> None:
    logger.info("Loading dataset | path=%s", DATASET_PATH)

    try:
        df = load_dataset(DATASET_PATH)

        logger.info(
            "Dataset loaded | records=%s | meters=%s | households=%s | zones=%s",
            f"{len(df):,}",
            df["Meter ID"].nunique(),
            df["Household ID"].nunique(),
            sorted(df["Grid Zone"].unique().tolist()),
        )

        logger.info(
            "Dataset time range | start=%s | end=%s",
            df["Timestamp"].min(),
            df["Timestamp"].max(),
        )

        logger.info("Transforming dataset into Kafka events...")
        events = transform_dataset(df)

        logger.info("Transformation completed | events=%s", f"{len(events):,}")
        producer = create_producer()
        logger.info("Starting to produce events to Kafka | topic=%s", KAFKA_TOPIC)
        produce_events(producer=producer, events=events)

    except KeyboardInterrupt:
        logger.info("Producer stopped by user")

    except Exception:
        logger.exception("Producer terminated unexpectedly")
        sys.exit(1)

    finally:
        if producer is not None:
            logger.info("Flushing pending Kafka messages...")
            producer.flush(10)

if __name__ == "__main__":
    main()
