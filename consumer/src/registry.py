import logging
from dataclasses import dataclass

from consumers.kafka_consumer import KafkaConsumerClient
from pipeline import Pipeline
from sinks.factory import build_sink

logger = logging.getLogger(__name__)


@dataclass
class PipelineRunner:
    """Bundles the Kafka consumer + Pipeline that manager.py runs."""
    name: str
    consumer: KafkaConsumerClient
    pipeline: Pipeline


def build_pipelines(application: dict, pipelines_config: dict) -> list[PipelineRunner]:
    kafka_cfg = application["kafka"]
    runners: list[PipelineRunner] = []

    for entry in pipelines_config.get("pipelines", []):
        name = entry["name"]

        if not entry.get("enabled", False):
            logger.info("Skipping disabled pipeline '%s'", name)
            continue

        if entry.get("type") == "spark":
            # Spark Structured Streaming pipelines run as their own jobs,
            # not inside this consumer process. See pipelines.yaml.
            logger.info("Skipping Spark pipeline '%s' - not managed by this process", name)
            continue

        sink_type = entry["sink"]["type"]
        connection_cfg = application.get(sink_type, {})
        pipeline_sink_cfg = {k: v for k, v in entry["sink"].items() if k != "type"}
        sink = build_sink(sink_type, {**connection_cfg, **pipeline_sink_cfg})

        batch_cfg = entry.get("batch", {})
        pipeline = Pipeline(
            name=name,
            sink=sink,
            batch_size=batch_cfg.get("size", 500),
            flush_interval_ms=batch_cfg.get("flush_interval_ms", 1000),
            max_buffer_size=batch_cfg.get("max_buffer_size", 5000),
            datetime_fields=entry.get("datetime_fields", []),
        )

        consumer = KafkaConsumerClient(
            bootstrap_servers=kafka_cfg["bootstrap_servers"],
            group_id = entry.get("consumer_group",kafka_cfg.get("consumer_group", f"{name}-group")
),
            topic=entry["topic"],
        )

        runners.append(PipelineRunner(name=name, consumer=consumer, pipeline=pipeline))
        logger.info("Built pipeline '%s' (topic=%s, sink=%s)", name, entry["topic"], sink_type)

    return runners
