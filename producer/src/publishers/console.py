import json

from publishers.base import Publisher


class ConsolePublisher(Publisher):
    """Prints transactions to stdout - used for local testing, so this
    intentionally uses print() rather than logging."""

    def publish(self, transaction):
        print(json.dumps(transaction.to_dict(), indent=2, default=str))

    def close(self):
        pass
