from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC
)

from .console import ConsolePublisher
from .kafka import KafkaPublisher


def get_publisher(name):

    if name == "console":
        return ConsolePublisher()

    if name == "kafka":
        return KafkaPublisher(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC
        )

    raise ValueError(
        f"Unknown publisher: {name}"
    )