from pipelines.base import Pipeline
from sinks.factory import SinkFactory


class OzonePipeline(Pipeline):

    def __init__(self, config):

        super().__init__(config)

        self.sink = SinkFactory.create(config.sink)

    def _flush_batch(self, records) -> bool:

        return self.sink.write(records)