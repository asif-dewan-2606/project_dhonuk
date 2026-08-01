from abc import ABC, abstractmethod


class Sink(ABC):

    def __init__(self, config):

        self.config = config

    @abstractmethod
    def write(self, messages) -> bool:
        """
        Persist a batch of messages.

        Returns:
            True  -> write successful
            False -> write failed
        """
        pass

    def close(self):
        """
        Override if the sink needs cleanup.
        """
        pass