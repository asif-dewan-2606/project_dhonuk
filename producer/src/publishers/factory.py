from publishers.console import ConsolePublisher
from publishers.kafka import KafkaPublisher


def get_publisher(name):

    if name == "console":
        return ConsolePublisher()

    if name == "kafka":
        return KafkaPublisher()

    raise ValueError(f"Unknown publisher: {name}")