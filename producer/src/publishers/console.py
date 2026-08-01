import json

from .base import Publisher


class ConsolePublisher(Publisher):

    def publish(self, message):

        print(
            json.dumps(
                message.to_dict(),
                indent=2
            )
        )
    
    def close(self):
        pass