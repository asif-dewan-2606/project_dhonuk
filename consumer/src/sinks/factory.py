from sinks.base import Sink
from sinks.clickhouse import ClickHouseSink
from sinks.ozone import OzoneSink
from sinks.postgres import PostgresSink

_SINK_TYPES = {
    "clickhouse": ClickHouseSink,
    "postgres": PostgresSink,
    "ozone": OzoneSink,
}


def build_sink(sink_type: str, sink_config: dict) -> Sink:
    """
    sink_config is the merge of application.yaml's connection block
    (host/port/credentials) and the pipeline's own sink block
    (table/columns/bucket/etc) - see registry.py.
    """
    sink_cls = _SINK_TYPES.get(sink_type)

    if sink_cls is None:
        raise ValueError(
            f"Unknown sink type '{sink_type}'. Known types: {list(_SINK_TYPES)}"
        )

    return sink_cls(**sink_config)
