import logging

from confluent_kafka import Consumer

logger = logging.getLogger(__name__)


class KafkaConsumerClient:
    """
    Thin wrapper around confluent-kafka Consumer.

    Responsibilities: connect, subscribe, poll, commit, close.
    Nothing about batching or sinks belongs here.
    """

    def __init__(self, bootstrap_servers: str, group_id: str, topic: str):
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )
        self.consumer.subscribe([topic])
        self.topic = topic
        logger.info("Subscribed to topic '%s' (group=%s)", topic, group_id)

    def poll(self, timeout: float = 1.0):
        return self.consumer.poll(timeout)

    def commit(self):
        self.consumer.commit(asynchronous=False)

    def close(self):
        self.consumer.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
