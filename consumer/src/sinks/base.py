from abc import ABC, abstractmethod
from typing import Any


class Sink(ABC):
    """
    A destination for batches of records.

    Responsibilities: convert records to storage-specific format, write,
    retry on failure, close resources. Nothing else - buffering and
    batching live in Pipeline, not here.
    """

    @abstractmethod
    def write(self, records: list[dict[str, Any]]) -> None:
        """
        Write a batch of records. Must raise on failure (after any internal
        retries are exhausted) so the caller never commits Kafka offsets
        for a batch that didn't actually land.
        """

    @abstractmethod
    def close(self) -> None: ...
