import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class JsonStateStore:
    """
    Persists a single JSON-serializable watermark value per source to a
    local file. Deliberately simple - one file per source, atomic
    write-then-rename so a crash mid-write can never corrupt it.

    This is a stopgap: the plan is to move watermark storage into a
    database table once the producer runs as more than a single instance.
    """

    def __init__(self, path: str, default: Any):
        self.path = Path(path)
        self.default = default
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> Any:
        if not self.path.exists():
            return self.default

        try:
            with open(self.path, "r") as f:
                return json.load(f)["watermark"]
        except (json.JSONDecodeError, KeyError, OSError):
            logger.exception("Failed to read state file %s - using default", self.path)
            return self.default

    def save(self, watermark: Any) -> None:
        tmp_fd, tmp_path = tempfile.mkstemp(dir=self.path.parent, prefix=".tmp-")
        try:
            with os.fdopen(tmp_fd, "w") as f:
                json.dump({"watermark": watermark}, f, default=str)
            os.replace(tmp_path, self.path)
        except Exception:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise
