import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError

from sinks.base import Sink

logger = logging.getLogger(__name__)


class OzoneSink(Sink):

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        prefix: str,
        extension: str = "ndjson",
        max_retries: int = 3,
        retry_backoff_seconds: float = 2.0,
    ):
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.extension = extension

        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds

        self.client = boto3.client(
            service_name="s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",
        )

    def _build_key(self) -> str:
        now = datetime.now(timezone.utc)

        return (
            f"{self.prefix}/"
            f"year={now:%Y}/"
            f"month={now:%m}/"
            f"day={now:%d}/"
            f"hour={now:%H}/"
            f"part-{now:%Y%m%dT%H%M%S}-{uuid.uuid4().hex}.{self.extension}"
        )

    def write(self, records: list[dict[str, Any]]) -> None:

        if not records:
            return

        key = self._build_key()

        body = "\n".join(
            json.dumps(record, default=str)
            for record in records
        ).encode("utf-8")

        attempt = 0

        while True:

            attempt += 1

            try:

                self.client.put_object(
                    Bucket=self.bucket,
                    Key=key,
                    Body=body,
                    ContentType="application/x-ndjson",
                )

                logger.info(
                    "Uploaded %d records to s3://%s/%s",
                    len(records),
                    self.bucket,
                    key,
                )

                return

            except ClientError:

                logger.exception(
                    "Ozone upload failed (attempt %d/%d)",
                    attempt,
                    self.max_retries,
                )

                if attempt >= self.max_retries:
                    raise

                time.sleep(self.retry_backoff_seconds * attempt)

    def close(self) -> None:
        pass