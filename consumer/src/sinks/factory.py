from sinks.clickhouse import ClickHouseSink
# from sinks.ozone import OzoneSink


class SinkFactory:

    _SINKS = {
        "clickhouse": ClickHouseSink,
        # "postgres": PostgresSink,
        # "ozone": OzoneSink,
    }

    @classmethod
    def create(cls, pipeline_config):

        sink_type = pipeline_config.sink.type

        sink_class = cls._SINKS.get(sink_type)

        if sink_class is None:
            raise ValueError(f"Unsupported sink: {sink_type}")

        return sink_class(pipeline_config)