import logging
import time

from confluent_kafka import Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
import json

from publishers.base import Publisher

logger = logging.getLogger(__name__)


class KafkaPublisher(Publisher):

    def __init__(self, bootstrap_servers, topic):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic

        self._wait_for_kafka()

        self.producer = Producer({"bootstrap.servers": bootstrap_servers})
        self._ensure_topic_exists(self.topic)

    def _delivery_report(self, err, msg):
        if err:
            logger.error("Delivery failed: %s", err)

    
    def publish(self, transaction, topic=None):
        topic = topic or self.topic

        self.producer.produce(
            topic,
            value=transaction.to_json().encode("utf-8"),
            callback=self._delivery_report,
        )

        self.producer.poll(0)

    def _ensure_topic_exists(self, topic_name):
        admin = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        metadata = admin.list_topics(timeout=10)

        if topic_name in metadata.topics:
            logger.info("Topic '%s' already exists", topic_name)
            return

        topic = NewTopic(topic=topic_name, num_partitions=3, replication_factor=1)
        futures = admin.create_topics([topic])

        for _, future in futures.items():
            future.result()

        logger.info("Topic '%s' created", topic_name)
    def close(self):
        logger.info("Flushing pending messages...")
        self.producer.flush()
        logger.info("Kafka producer closed")

    def _wait_for_kafka(self, max_retries=30, retry_interval=2):
        admin = AdminClient({"bootstrap.servers": self.bootstrap_servers})

        for attempt in range(1, max_retries + 1):
            try:
                admin.list_topics(timeout=5)
                logger.info("Connected to Kafka")
                return
            except KafkaException:
                logger.warning(
                    "Kafka not ready (%d/%d). Retrying in %ds...",
                    attempt, max_retries, retry_interval,
                )
                time.sleep(retry_interval)

        raise RuntimeError("Unable to connect to Kafka after multiple retries")
