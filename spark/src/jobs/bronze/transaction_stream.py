from session import get_spark
from readers.ozone import read_raw_stream
from writers.iceberg import merge


SOURCE_PATH = "transaction_stream"
TARGET_TABLE = "polaris.bronze.transaction_stream"
CHECKPOINT_PATH = "s3a://raw/checkpoints/raw_to_bronze"


class RawToBronzeJob:

    def __init__(self, config):
        self.config = config

    def run(self):

        spark = get_spark("raw-to-bronze")

        try:
            # Use the existing Bronze table schema for the streaming reader
            schema = spark.table(TARGET_TABLE).schema

            df = read_raw_stream(
                spark,
                SOURCE_PATH,
                schema,
            )

            def process_batch(batch_df, batch_id):
                print("=" * 60)
                print(f"Processing Bronze batch: {batch_id}")
                print("=" * 60)

                if batch_df.isEmpty():
                    print("Batch is empty")
                    return

                merge(
                    batch_df,
                    TARGET_TABLE,
                    ["txn_id", "id"],
                )

                print(f"Bronze batch {batch_id} completed")

            query = (
                df.writeStream
                .foreachBatch(process_batch)
                .option("checkpointLocation", CHECKPOINT_PATH)
                .trigger(availableNow=True)
                .start()
            )

            query.awaitTermination()

        except Exception:
            print("Raw-to-Bronze job failed")
            raise

        finally:
            spark.stop()