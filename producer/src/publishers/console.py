import json

from .base import Publisher


class ConsolePublisher(Publisher):

    def publish(self, transaction):

        print(
            json.dumps(
                transaction.to_dict(),
                indent=2
            )
        )
    
    def close(self):
        pass