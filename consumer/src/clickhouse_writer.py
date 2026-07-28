import clickhouse_connect

from config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_TABLE,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
)


class ClickHouseWriter:
    """
    Writes batches into ClickHouse.
    """

    def __init__(self):
        self.client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DATABASE,
        )

    def insert(self, rows):
        """
        Insert a batch of rows into ClickHouse.
        """

        self.client.insert(
            table=CLICKHOUSE_TABLE,
            data=rows,
            column_names=[
                "id",
                "sqn",
                "approval_date",
                "approval_datetime",
                "nr_number",
                "response_code",
                "status",
                "txn_type",
                "processing_code",
                "txn_type_d_c",
                "txn_cat",
                "pos_entry_mode",
                "par",
                "target_par",
                "txn_amt",
                "acc_blc",
                "acc_available_blc",
                "user_id",
                "customer_segment",
                "trust_level",
                "target_user_id",
                "target_customer_segment",
                "target_trust_level",
                "target_account_type",
                "account_id1",
                "created",
                "updated",
                "txn_id",
                "account_type",
                "txn_sub_type",
                "org_nr_number",
                "sync_flag",
                "rrnum",
                "stan",
            ],
        )

    def close(self):
        self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()