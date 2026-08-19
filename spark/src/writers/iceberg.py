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


def merge(
    df: DataFrame,
    table: str,
    keys: list[str],
):
    """
    Upsert a DataFrame into an Iceberg table.

    Rows matching all key columns are updated.
    Rows with no matching key are inserted.
    """

    temp_view = "_merge_source"

    df.createOrReplaceTempView(temp_view)

    columns = df.columns

    # target.txn_id = source.txn_id
    # AND target.id = source.id
    match_condition = " AND ".join(
        f"target.`{key}` = source.`{key}`"
        for key in keys
    )

    # target.col = source.col
    update_assignments = ", ".join(
        f"target.`{column}` = source.`{column}`"
        for column in columns
    )

    column_list = ", ".join(
        f"`{column}`"
        for column in columns
    )

    value_list = ", ".join(
        f"source.`{column}`"
        for column in columns
    )

    sql = f"""
        MERGE INTO {table} AS target
        USING {temp_view} AS source
        ON {match_condition}

        WHEN MATCHED THEN
            UPDATE SET {update_assignments}

        WHEN NOT MATCHED THEN
            INSERT ({column_list})
            VALUES ({value_list})
    """

    df.sparkSession.sql(sql)

    # Remove the temporary view after the merge
    df.sparkSession.catalog.dropTempView(temp_view)