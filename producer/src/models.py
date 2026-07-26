from dataclasses import dataclass, asdict
from datetime import datetime


@dataclass
class Transaction:
    transaction_id: str
    customer_id: int
    merchant_id: int
    amount: float
    status: str
    transaction_time: str

    def to_dict(self):
        return asdict(self)
    
    def to_json(self):

        import json

        return json.dumps(self.to_dict())
        