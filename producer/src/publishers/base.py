from abc import ABC, abstractmethod


class Publisher(ABC):

    @abstractmethod
    def publish(self, transaction):
        pass

    @abstractmethod
    def close(self):
        pass
