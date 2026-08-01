from pipelines.base import Pipeline


class SparkPipeline(Pipeline):

    def process(self, messages):
        print(f"SparkPipeline received {len(messages)} messages")