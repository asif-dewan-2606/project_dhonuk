from confluent_kafka import Consumer

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_CONSUMER_GROUP,
)


class KafkaConsumerClient:
    """
    Wrapper around confluent-kafka Consumer.

    Responsibilities:
        - Connect to Kafka
        - Subscribe to topic
        - Poll messages
        - Commit offsets
        - Close consumer
    """

    def __init__(self, topic=KAFKA_TOPIC):
        self.consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "group.id": KAFKA_CONSUMER_GROUP,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

        self.consumer.subscribe([topic])

    def poll(self, timeout=1.0):
        """
        Poll a single message from Kafka.
        """
        return self.consumer.poll(timeout)

    def commit(self):
        """
        Commit processed offsets.
        """
        self.consumer.commit(asynchronous=False)

    def close(self):
        """
        Close Kafka consumer.
        """
        self.consumer.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()