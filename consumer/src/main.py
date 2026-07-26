import json

from consumer import KafkaConsumerClient
from batch import BatchBuffer
from clickhouse_writer import ClickHouseWriter


def main():

    with KafkaConsumerClient() as consumer, ClickHouseWriter() as writer:

        buffer = BatchBuffer()

        while True:

            msg = consumer.poll()

            if msg is None:
                continue

            if msg.error():
                print(msg.error())
                continue

            record = json.loads(msg.value().decode("utf-8"))

            row = (
                record["transaction_id"],
                record["customer_id"],
                record["merchant_id"],
                record["amount"],
                record["status"],
                record["transaction_time"],
            )

            buffer.add(row)

            if buffer.is_ready():

                writer.insert(buffer.get_batch())

                consumer.commit()

                buffer.clear()


if __name__ == "__main__":
    main()