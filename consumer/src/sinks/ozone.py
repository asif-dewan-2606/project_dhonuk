import logging
from typing import Any

from sinks.base import Sink

logger = logging.getLogger(__name__)


class OzoneSink(Sink):
    """
    Placeholder for the Kafka -> Apache Ozone pipeline.

    Ozone speaks S3-compatible API, so write() will likely batch records
    into a file (parquet/json) and upload via boto3 when implemented.
    Not wired into requirements.txt yet - add boto3 when you do.
    """

    def __init__(self, endpoint: str, bucket: str, path_prefix: str = ""):
        self.endpoint = endpoint
        self.bucket = bucket
        self.path_prefix = path_prefix

    def write(self, records: list[dict[str, Any]]) -> None:
        raise NotImplementedError("OzoneSink.write is not implemented yet")

    def close(self) -> None:
        pass
