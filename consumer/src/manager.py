from consumers.kafka_consumer import KafkaConsumer
from registry import PipelineRegistry


class ConsumerManager:

    def __init__(self):

        self.registry = PipelineRegistry()
        print(f"Registry topic: {self.registry.topic()}", flush=True)

        self.pipeline = self.registry.get()

        self.consumer = KafkaConsumer(
            [self.registry.topic()]
        )

    def start(self):
        print("Consumer started.", flush=True)

        try:

            while True:

                messages = self.consumer.consume(1000)
                print(f"Kafka returned {len(messages)} messages", flush=True)

                if not messages:
                    continue

                for message in messages:

                    if message is None:
                        continue

                    if message.error():
                        print(message.error())
                        continue

                    last_message = self.pipeline.add(message)

                    if last_message is not None:
                        self.consumer.commit(last_message)

        finally:

            last_message = self.pipeline.close()

            if last_message is not None:
                self.consumer.commit(last_message)

            self.consumer.close()