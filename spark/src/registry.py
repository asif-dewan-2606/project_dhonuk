from jobs.bronze.transaction_stream import RawToBronzeJob
from jobs.silver.cashin_transaction import BronzeToSilverCashinJob
from jobs.silver.cashout_transaction import BronzeToSilverCashoutJob 
from jobs.gold.cashin_summary import SilverToGoldCashinJob
from jobs.gold.cashout_summary import SilverToGoldCashoutJob

class JobRegistry:

    def __init__(self):
        self._jobs = {
            "raw_to_bronze": RawToBronzeJob,
            "bronze_to_silver_cashin": BronzeToSilverCashinJob,
            "bronze_to_silver_cashout": BronzeToSilverCashoutJob,
            "silver_to_gold_cashin": SilverToGoldCashinJob,
            "silver_to_gold_cashout": SilverToGoldCashoutJob,
        }

    def get(self, name):

        if name not in self._jobs:
            raise ValueError(f"Unknown job: {name}")

        return self._jobs[name]