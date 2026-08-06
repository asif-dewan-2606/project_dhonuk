from config.config import pipelines
from registry import JobRegistry


class SparkManager:

    def __init__(self):

        self.registry = JobRegistry()

    def run(self):

        for job in pipelines():

            if not job["enabled"]:
                continue

            job_class = self.registry.get(job["name"])

            instance = job_class(job)

            instance.run()