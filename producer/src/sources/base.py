from abc import ABC, abstractmethod
from typing import Any


class Source(ABC):
    """
    Produces a batch of records each time it's polled.

    Responsibilities: connect, fetch, wrap rows into something a Publisher
    can serialize (see models.Record), close. Rate limiting between polls
    lives in manager.py, not here - a Source just reports how often it
    wants to be polled via poll_interval_seconds.
    """

    poll_interval_seconds: float

    @abstractmethod
    def poll(self) -> list[Any]:
        """
        Return zero or more new records since the last call. Must not raise
        just because there's nothing new - only on genuine connection/query
        failures.
        """

    @abstractmethod
    def close(self) -> None: ...
