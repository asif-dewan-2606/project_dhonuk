from session import get_spark


class BronzeToSilverCashoutJob:

    def __init__(self, config):
        self.config = config

    def run(self):

        job_name = self.config["name"]

        spark = get_spark(f"dhonuk-{job_name}")

        try:
            source_table = self.config["source"]["table"]
            target_table = self.config["sink"]["table"]
            processing_code = self.config["filter"]["processing_code"]

            print("=" * 60)
            print("Starting Bronze → Silver")
            print(f"Job    : {job_name}")
            print(f"Source : {source_table}")
            print(f"Target : {target_table}")
            print(f"Filter : processing_code = {processing_code}")
            print("=" * 60)

            df = spark.table(source_table)

            result = df.filter(
                df.processing_code == processing_code
            )

            result.writeTo(target_table).createOrReplace()

            print("=" * 60)
            print("Silver job completed successfully")
            print("=" * 60)

        except Exception:
            print(f"Job failed: {job_name}")
            raise

        finally:
            spark.stop()