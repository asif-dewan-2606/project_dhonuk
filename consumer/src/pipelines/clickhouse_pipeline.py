from pipelines.base import Pipeline
from sinks.factory import SinkFactory


class ClickHousePipeline(Pipeline):

    def __init__(self, config):

        super().__init__(config)

        self.sink = SinkFactory.create(config)

    def _flush_batch(self, records) -> bool:

        return self.sink.write(records)