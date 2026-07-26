# import json


# class ConsolePublisher:

#     def publish(self, transaction):

#         print(
#             json.dumps(
#                 transaction.to_dict(),
#                 indent=2
#             )
#         )


import json
from abc import ABC, abstractmethod


class Publisher(ABC):

    @abstractmethod
    def publish(self, transaction):
        pass


class ConsolePublisher(Publisher):

    def publish(self, transaction):

        print(
            json.dumps(
                transaction.to_dict(),
                indent=2
            )
        )


def get_publisher(publisher_type: str) -> Publisher:

    if publisher_type == "console":
        return ConsolePublisher()

    raise ValueError(
        f"Unknown publisher type: {publisher_type}"
    )