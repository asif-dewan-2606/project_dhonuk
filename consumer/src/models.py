from dataclasses import dataclass
from confluent_kafka import Message


@dataclass
class FlushResult:
    flushed: bool = False
    last_message: Message | None = None