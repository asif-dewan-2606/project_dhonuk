import json

from consumer import KafkaConsumerClient
from batch import BatchBuffer
from clickhouse_writer import ClickHouseWriter
from datetime import datetime



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
            # print(msg.value().decode("utf-8"))

            record = json.loads(msg.value().decode("utf-8"))
            

            row = (
                record["id"],
                record["sqn"],
                record["approval_date"],
                datetime.fromisoformat(record["approval_datetime"]),
                record["nr_number"],
                record["response_code"],
                record["status"],
                record["txn_type"],
                record["processing_code"],
                record["txn_type_d_c"],
                record["txn_cat"],
                record["pos_entry_mode"],
                record["par"],
                record["target_par"],
                record["txn_amt"],
                record["acc_blc"],
                record["acc_available_blc"],
                record["user_id"],
                record["customer_segment"],
                record["trust_level"],
                record["target_user_id"],
                record["target_customer_segment"],
                record["target_trust_level"],
                record["target_account_type"],
                record["account_id1"],
                datetime.fromisoformat(record["created"]),
                datetime.fromisoformat(record["updated"]) if record["updated"] else None,
                record["txn_id"],
                record["account_type"],
                record["txn_sub_type"],
                record["org_nr_number"],
                record["sync_flag"],
                record["rrnum"],
                record["stan"],
            )

            buffer.add(row)

            if buffer.is_ready():
                

                writer.insert(buffer.get_batch())

                consumer.commit()

                buffer.clear()


if __name__ == "__main__":
    main()