from config.config import pipelines
from registry import JobRegistry


class SparkManager:

    def __init__(self):
        self.registry = JobRegistry()

    def run(self, job_name=None):

        for job in pipelines():

            if not job["enabled"]:
                continue

            # If a specific job was requested, skip everything else
            if job_name and job["name"] != job_name:
                continue

            job_class = self.registry.get(job["name"])

            instance = job_class(job)

            instance.run()