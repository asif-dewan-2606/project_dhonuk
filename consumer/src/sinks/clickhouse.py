import logging
import time
from typing import Any

import clickhouse_connect

from sinks.base import Sink

logger = logging.getLogger(__name__)


class ClickHouseSink(Sink):
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        table: str,
        columns: list[str],
        max_retries: int = 3,
        retry_backoff_seconds: float = 2.0,
    ):
        self.table = table
        self.columns = columns
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds

        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database,
        )

    def write(self, records: list[dict[str, Any]]) -> None:
        if not records:
            return

        rows = [[record.get(col) for col in self.columns] for record in records]

        attempt = 0
        while True:
            attempt += 1
            try:    

                self.client.insert(table=self.table, data=rows, column_names=self.columns)
                return
            except Exception:
                logger.exception(
                    "ClickHouse insert failed (attempt %d/%d, %d rows)",
                    attempt, self.max_retries, len(rows),
                )
                if attempt >= self.max_retries:
                    raise
                time.sleep(self.retry_backoff_seconds * attempt)

    def close(self) -> None:
        self.client.close()
