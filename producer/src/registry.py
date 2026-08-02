import logging
from dataclasses import dataclass
from typing import Any

from config import KAFKA_TOPIC
from publishers.factory import get_publisher
from sources.base import Source
from sources.factory import build_source

logger = logging.getLogger(__name__)


@dataclass
class ProducerRunner:
    """Bundles the Source + Publisher that manager.py drives for one pipeline."""
    name: str
    source: Source
    publisher: Any
    poll_interval_seconds: float


def build_pipelines(sources_config: dict) -> list[ProducerRunner]:
    runners: list[ProducerRunner] = []

    for entry in sources_config.get("sources", []):
        name = entry["name"]

        if not entry.get("enabled", False):
            logger.info("Skipping disabled source '%s'", name)
            continue

        source = build_source(entry)
        publisher = get_publisher(
            entry.get("publisher", "kafka"),
            topic=entry.get("topic", KAFKA_TOPIC),
        )

        runners.append(
            ProducerRunner(
                name=name,
                source=source,
                publisher=publisher,
                poll_interval_seconds=source.poll_interval_seconds,
            )
        )
        logger.info(
            "Built producer pipeline '%s' (type=%s, topic=%s)",
            name, entry["type"], entry.get("topic", KAFKA_TOPIC),
        )

    return runners
