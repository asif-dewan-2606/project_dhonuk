from confluent_kafka import Consumer

from config import config


class KafkaConsumer:

    def __init__(self, topics: list[str]):

        print("Bootstrap:", config.kafka.bootstrap_servers, flush=True)
        print("Group:", config.kafka.consumer_group, flush=True)
        print("Topics:", topics, flush=True)
        print("Offset Reset:", config.kafka.auto_offset_reset, flush=True)

        self.consumer = Consumer(
            {
                "bootstrap.servers": ",".join(config.kafka.bootstrap_servers),
                "group.id": config.kafka.consumer_group,
                "auto.offset.reset": config.kafka.auto_offset_reset,
                "enable.auto.commit": config.kafka.enable_auto_commit,
            }
        )

        print(f"Subscribing to: {topics}", flush=True)
        self.consumer.subscribe(topics)
        print("Subscribed.", flush=True)

    def consume(self, batch_size: int, timeout=None):

        print("Calling Kafka consume...", flush=True)

        timeout = timeout or (
            config.consumer.poll_timeout_ms / 1000
        )

        return self.consumer.consume(
            num_messages=batch_size,
            timeout=timeout,
        )

    def commit(self, message):

        self.consumer.commit(
            message=message,
            asynchronous=False
        )

    def close(self):

        self.consumer.close()