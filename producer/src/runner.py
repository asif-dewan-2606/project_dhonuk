class Runner:

    def __init__(self, pipeline):
        self.pipeline = pipeline

    def run(self):
        try:
            for message in self.pipeline.source.fetch():
                self.pipeline.publisher.publish(message)

        finally:
            self.pipeline.publisher.close()