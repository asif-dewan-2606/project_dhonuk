from abc import ABC, abstractmethod
from collections.abc import Iterator


class Source(ABC):

    @abstractmethod
    def fetch(self) -> Iterator:
        """
        Infinite generator that yields messages.
        """
        ...

    def close(self):
        """
        Optional cleanup.
        """
        pass