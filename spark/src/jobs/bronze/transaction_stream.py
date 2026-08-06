from session import get_spark
from readers.ozone import read_raw
from writers.iceberg import overwrite


class RawToBronzeJob:

    def __init__(self, config):
        self.config = config

    def run(self):

        spark = get_spark("raw-to-bronze")

        print("=" * 60)
        print("Reading Raw Layer...")
        print("=" * 60)

        df = read_raw(
            spark,
            "transaction_stream"
        )



        overwrite(
            df,
            "polaris.bronze.transaction_stream"
        )

        print("=" * 60)
        print("Bronze Updated")
        print("=" * 60)

        spark.stop()