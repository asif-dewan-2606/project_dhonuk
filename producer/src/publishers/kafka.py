import json
import time
from .base import Publisher

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException

class KafkaPublisher(Publisher):

    def __init__(
        self,
        bootstrap_servers,
        topic
    ):

        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.wait_for_kafka()

        self.producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers
            }
        )
        
        self.ensure_topic_exists()

    def delivery_report(self, err, msg):

        if err:
            print(f"Delivery failed: {err}")

    def publish(self, message):

        self.producer.produce(
            topic=self.topic,
            value=message.to_json(),
            callback=self.delivery_report
        )

        self.producer.poll(0)
    
    def ensure_topic_exists(self):

        admin = AdminClient(
            {
                "bootstrap.servers": self.bootstrap_servers
            }
        )

        metadata = admin.list_topics(timeout=10)

        if self.topic in metadata.topics:
            print(f"Topic '{self.topic}' already exists.")
            return

        topic = NewTopic(
            topic=self.topic,
            num_partitions=3,
            replication_factor=1
        )

        futures = admin.create_topics([topic])

        for _, future in futures.items():
            future.result()

        print(f"Topic '{self.topic}' created.")
    
    def close(self):

        print("Flushing pending messages...")

        self.producer.flush()

        print("Kafka producer closed.")

    def wait_for_kafka(self, max_retries=30, retry_interval=2):

        admin = AdminClient(
            {
                "bootstrap.servers": self.bootstrap_servers
            }
        )

        for attempt in range(1, max_retries + 1):

            try:

                admin = AdminClient(
                    {
                        "bootstrap.servers": self.bootstrap_servers
                    }
                )

                admin.list_topics(timeout=5)

                print("Connected to Kafka.")

                return

            except KafkaException:


                print(
                    f"Kafka not ready "
                    f"({attempt}/{max_retries}). "
                    f"Retrying in {retry_interval} seconds..."
                )

                time.sleep(retry_interval)

        raise RuntimeError(
            "Unable to connect to Kafka after multiple retries."
        )