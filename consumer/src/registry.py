from config import pipeline
from pipelines.factory import PipelineFactory


class PipelineRegistry:

    def __init__(self):

        if not pipeline.enabled:
            raise ValueError("Pipeline is disabled.")

        self._pipeline = PipelineFactory.create(pipeline)

    def get(self):

        return self._pipeline

    def topic(self):

        return self._pipeline.topic

    def close(self):

        self._pipeline.close()