import time

from config import EVENTS_PER_SECOND, PUBLISHER
from generator import TransactionGenerator
from publishers.factory import get_publisher


generator = TransactionGenerator()
publisher = get_publisher(PUBLISHER)

interval = 1 / EVENTS_PER_SECOND

def main():

    generator = TransactionGenerator()
    publisher = get_publisher(PUBLISHER)

    try:

        while True:

            transaction = generator.generate()

            publisher.publish(transaction)

            time.sleep(1 / EVENTS_PER_SECOND)

    except KeyboardInterrupt:

        print("\nStopping producer...")

    finally:

        publisher.close()


if __name__ == "__main__":
    main()