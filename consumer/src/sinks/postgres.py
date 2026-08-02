import logging
from typing import Any

from sinks.base import Sink

logger = logging.getLogger(__name__)


class PostgresSink(Sink):
    """
    Placeholder for the Kafka -> Postgres pipeline.

    Kept minimal on purpose: implement write() with psycopg2 (or
    psycopg3) execute_values / COPY when this pipeline is actually needed.
    Not wired into requirements.txt yet - add psycopg2-binary when you do.
    """

    def __init__(self, host: str, port: int, database: str, user: str, password: str, table: str, columns: list[str]):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.table = table
        self.columns = columns

    def write(self, records: list[dict[str, Any]]) -> None:
        raise NotImplementedError("PostgresSink.write is not implemented yet")

    def close(self) -> None:
        pass
