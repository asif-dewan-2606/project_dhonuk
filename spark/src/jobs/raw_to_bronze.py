from pyspark.sql import SparkSession


class RawToBronzeJob:

    def __init__(self, config):
        self.config = config

    def run(self):

        spark = (
            SparkSession.builder
            .appName("dhonuk-raw-to-bronze")
            .master("local[*]")

            # ---------- Iceberg ----------
            .config(
                "spark.jars",
                ",".join([
                    "/opt/spark/jars/custom/iceberg-spark-runtime-4.0_2.13-1.10.0.jar",
                    "/opt/spark/jars/custom/iceberg-aws-bundle-1.10.0.jar",
                    "/opt/spark/jars/custom/hadoop-aws-3.4.1.jar",
                    "/opt/spark/jars/custom/bundle-2.30.31.jar",
                ])
            )

            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            )

            .config(
                "spark.sql.catalog.polaris",
                "org.apache.iceberg.spark.SparkCatalog"
            )

            .config(
                "spark.sql.catalog.polaris.type",
                "rest"
            )

            .config(
                "spark.sql.catalog.polaris.uri",
                "http://polaris:8181/api/catalog"
            )

            .config(
                "spark.sql.catalog.polaris.warehouse",
                "s3://raw"
            )

            # ---------- Ozone ----------
            .config(
                "spark.hadoop.fs.s3a.endpoint",
                "http://ozone-s3g:9878"
            )

            .config(
                "spark.hadoop.fs.s3a.access.key",
                "ozone"
            )

            .config(
                "spark.hadoop.fs.s3a.secret.key",
                "ozone"
            )

            .config(
                "spark.hadoop.fs.s3a.path.style.access",
                "true"
            )

            .config(
                "spark.hadoop.fs.s3a.connection.ssl.enabled",
                "false"
            )

            .getOrCreate()
        )

        print("=" * 60)
        print("Spark Started")
        print(spark.version)
        print("=" * 60)

        spark.stop()