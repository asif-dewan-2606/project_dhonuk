from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

from publishers.console import ConsolePublisher
from publishers.kafka import KafkaPublisher

_PUBLISHER_TYPES = {
    "console": lambda: ConsolePublisher(),
    "kafka": lambda: KafkaPublisher(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topic=KAFKA_TOPIC,
    ),
}


def get_publisher(name):
    factory = _PUBLISHER_TYPES.get(name)

    if factory is None:
        raise ValueError(f"Unknown publisher: {name}")

    return factory()
