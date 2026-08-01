from threading import Thread

from runner import Runner


class PipelineManager:

    def __init__(self):
        self._threads = []

    def add_pipeline(self, pipeline):
        runner = Runner(pipeline)

        thread = Thread(
            target=runner.run,
            name=pipeline.name,
            daemon=True
        )

        self._threads.append(thread)

    def start(self):

        for thread in self._threads:
            thread.start()

        for thread in self._threads:
            thread.join()