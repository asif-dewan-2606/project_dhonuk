from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


def read_raw(
    spark: SparkSession,
    path: str,
    format: str = "json"
) -> DataFrame:

    return (
        spark.read
        .format(format)
        .option("recursiveFileLookup", "true")
        .load(f"s3a://raw/{path}")
    )