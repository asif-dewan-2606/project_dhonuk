import logging
import time

from config import EVENTS_PER_SECOND, PUBLISHER, LOG_LEVEL
from generator import TransactionGenerator
from publishers.factory import get_publisher

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    generator = TransactionGenerator()
    publisher = get_publisher(PUBLISHER)
    interval = 1 / EVENTS_PER_SECOND

    try:
        while True:
            transaction = generator.generate()

            # Existing topic
            publisher.publish(transaction)

            # Same transaction to another topic
            publisher.publish(transaction, "transaction_stream")
            
            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("Stopping producer...")

    finally:
        publisher.close()


if __name__ == "__main__":
    main()
