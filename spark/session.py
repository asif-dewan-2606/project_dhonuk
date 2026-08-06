from pyspark.sql import SparkSession


def get_spark(app_name: str) -> SparkSession:
    """
    Creates and returns a SparkSession.

    All Spark configuration is loaded automatically from:

        /opt/spark/conf/spark-defaults.conf
    """

    spark = (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )

    return spark