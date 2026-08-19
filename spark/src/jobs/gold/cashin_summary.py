from pyspark.sql import functions as F
from session import get_spark


class SilverToGoldCashinJob:

    def __init__(self, config):
        self.config = config

    def run(self):

        job_name = self.config["name"]

        spark = get_spark(f"dhonuk-{job_name}")

        try:
            source_table = self.config["source"]["table"]
            target_table = self.config["sink"]["table"]

            print("=" * 60)
            print("Starting Silver → Gold")
            print(f"Job    : {job_name}")
            print(f"Source : {source_table}")
            print(f"Target : {target_table}")
            print("=" * 60)

            df = spark.table(source_table)

            result = (
                df.groupBy("approval_date", "processing_code")
                  .agg(
                    F.count("txn_id").alias("transaction_count"),
                    F.sum("txn_amt").alias("total_amount")
                  )
            )

            result.writeTo(target_table).createOrReplace()

            print("=" * 60)
            print("Gold job completed successfully")
            print("=" * 60)

        except Exception:
            print(f"Job failed: {job_name}")
            raise

        finally:
            spark.stop()