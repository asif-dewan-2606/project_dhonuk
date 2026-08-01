from clickhouse_connect import get_client

from config import config
from sinks.base import Sink


class ClickHouseSink(Sink):

    def __init__(self, pipeline_config):

        super().__init__(pipeline_config)

        self.client = get_client(
            host=config.clickhouse.host,
            port=config.clickhouse.port,
            username=config.clickhouse.username,
            password=config.clickhouse.password,
        )

        self.database = pipeline_config.sink.database
        self.table = pipeline_config.sink.table

    def write(self, records):

        if not records:
            return True

        try:

            column_names = list(records[0].keys())

            rows = [
                [record[col] for col in column_names]
                for record in records
            ]

            self.client.insert(
                table=f"{self.database}.{self.table}",
                data=rows,
                column_names=column_names,
            )

            return True

        except Exception:
            import traceback
            traceback.print_exc()
            return False
    
    def close(self):

        self.client.close()