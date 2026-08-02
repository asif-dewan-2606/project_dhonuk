from generator import TransactionGenerator
from sources.base import Source


class GeneratorSource(Source):
    """Wraps the fake TransactionGenerator so local/dev testing runs
    through the exact same pipeline machinery as the real JDBC sources -
    no separate code path to keep in sync."""

    def __init__(self, events_per_second: float = 10):
        self.poll_interval_seconds = 1 / events_per_second
        self._generator = TransactionGenerator()

    def poll(self):
        return [self._generator.generate()]

    def close(self) -> None:
        pass
