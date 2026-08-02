import json
import logging
import time
from datetime import datetime
from typing import Any

from sinks.base import Sink

logger = logging.getLogger(__name__)


class Pipeline:
    """
    One Kafka topic -> one Sink, with its own buffering/batching.

    There is deliberately a single Pipeline class shared by every sink
    type (ClickHouse, Postgres, Ozone, ...). The buffering/flush logic is
    identical regardless of destination - only the Sink differs, and that
    varies through the `sink` argument, not subclassing. A Pipeline
    subclass per sink type would just be the same batching code copied
    three times.
    """

    def __init__(
        self,
        name: str,
        sink: Sink,
        batch_size: int,
        flush_interval_ms: int,
        max_buffer_size: int,
        datetime_fields: list[str] | None = None,
    ):
        self.name = name
        self.sink = sink
        self.batch_size = batch_size
        self.flush_interval_ms = flush_interval_ms
        self.max_buffer_size = max_buffer_size
        self.datetime_fields = datetime_fields or []

        self._buffer: list[dict[str, Any]] = []
        self._buffer_start: float | None = None

    def add(self, raw_value: bytes) -> None:
        record = json.loads(raw_value.decode("utf-8"))

        for field in self.datetime_fields:
            if record.get(field):
                record[field] = datetime.fromisoformat(record[field])

        self._buffer.append(record)

        if self._buffer_start is None:
            self._buffer_start = time.monotonic()

        if len(self._buffer) >= self.max_buffer_size:
            logger.warning(
                "[%s] max_buffer_size (%d) reached - flush is overdue",
                self.name, self.max_buffer_size,
            )

    def is_ready(self) -> bool:
        if not self._buffer:
            return False

        if len(self._buffer) >= self.batch_size:
            return True

        if len(self._buffer) >= self.max_buffer_size:
            return True

        elapsed_ms = (time.monotonic() - self._buffer_start) * 1000
        return elapsed_ms >= self.flush_interval_ms

    def flush(self) -> None:
        """
        Writes the buffer to the sink. Only clears the buffer on success -
        if sink.write() raises, the buffer is left intact so nothing is
        lost and the caller can decide whether to retry or crash.
        """
        if not self._buffer:
            return

        batch = self._buffer
        self.sink.write(batch)

        logger.debug("[%s] flushed %d records", self.name, len(batch))
        self._buffer = []
        self._buffer_start = None
