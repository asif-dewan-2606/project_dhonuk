from pyspark.sql import DataFrame


def overwrite(df: DataFrame, table: str):

    (
        df.writeTo(table)
        .using("iceberg")
        .createOrReplace()
    )


def append(df: DataFrame, table: str):

    (
        df.writeTo(table)
        .append()
    )