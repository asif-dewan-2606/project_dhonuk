from abc import ABC, abstractmethod


class Publisher(ABC):

    @abstractmethod
    def publish(self, message):
        """
        Publish a single message.
        """
        raise NotImplementedError

    @abstractmethod
    def close(self):
        raise NotImplementedError