from abc import ABC, abstractmethod
from time import monotonic
import json


class Pipeline(ABC):

    def __init__(self, config):

        self.config = config

        self.topic = config.topic

        self.buffer = []

        self.last_flush = monotonic()

    def add(self, message):

        self.buffer.append(message)

        if self._should_flush():
            return self.flush()

        return None

    def flush(self):

        if not self.buffer:
            return None

        last_message = self.buffer[-1]

        records = [
            self.deserialize(message)
            for message in self.buffer
        ]

        success = self._flush_batch(records)

        if not success:
            return None

        self.buffer.clear()

        self.last_flush = monotonic()

        return last_message

    def _should_flush(self):

        batch = self.config.batch

        return (
            len(self.buffer) >= batch.size
            or len(self.buffer) >= batch.max_buffer_size
            or (monotonic() - self.last_flush) >= batch.flush_interval
        )

    def deserialize(self, message):

        return json.loads(
            message.value().decode("utf-8")
        )

    @abstractmethod
    def _flush_batch(self, records) -> bool:
        pass

    def close(self):

        return self.flush()