import time

from config import BATCH_SIZE, BATCH_TIMEOUT_MS


class BatchBuffer:
    """
    In-memory batch buffer.

    Responsibilities:
        - Store records
        - Decide when batch is ready
        - Return current batch
        - Clear buffer
    """

    def __init__(self):
        self.records = []
        self.first_message_time = None

    def add(self, record):
        """
        Add a record to the batch.
        """
        if not self.records:
            self.first_message_time = time.monotonic()

        self.records.append(record)

    def is_ready(self):
        """
        Batch is ready if:
            - batch size reached
            - timeout reached
        """

        if not self.records:
            return False

        if len(self.records) >= BATCH_SIZE:
            return True

        elapsed_ms = (
            time.monotonic() - self.first_message_time
        ) * 1000

        return elapsed_ms >= BATCH_TIMEOUT_MS

    def get_batch(self):
        """
        Return current batch.
        """
        return self.records

    def clear(self):
        """
        Clear current batch.
        """
        self.records.clear()
        self.first_message_time = None

    def size(self):
        """
        Current number of buffered records.
        """
        return len(self.records)