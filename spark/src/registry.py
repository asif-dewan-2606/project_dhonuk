from jobs.bronze.transaction_stream import RawToBronzeJob


class JobRegistry:

    def __init__(self):
        self._jobs = {
            "raw_to_bronze": RawToBronzeJob,
        }

    def get(self, name):

        if name not in self._jobs:
            raise ValueError(f"Unknown job: {name}")

        return self._jobs[name]