from dataclasses import dataclass, asdict
from datetime import datetime
import json


@dataclass(slots=True)
class Transaction:
    id: int
    sqn: int
    approval_date: int
    approval_datetime: datetime
    nr_number: str
    response_code: str
    status: str
    txn_type: str | None
    processing_code: str
    txn_type_d_c: str | None
    txn_cat: str | None
    pos_entry_mode: str | None
    par: str
    target_par: str | None
    txn_amt: float
    acc_blc: float | None
    acc_available_blc: float | None
    user_id: str | None
    customer_segment: str | None
    trust_level: int | None
    target_user_id: str | None
    target_customer_segment: str | None
    target_trust_level: int | None
    target_account_type: str | None
    account_id1: str | None
    created: datetime
    updated: datetime | None
    txn_id: str | None
    account_type: str | None
    txn_sub_type: str | None
    org_nr_number: str | None
    sync_flag: int | None
    rrnum: str | None
    stan: int

    def to_dict(self):
        return asdict(self)


    def to_json(self):
        return json.dumps(
            self.to_dict(),
            default=lambda obj: obj.isoformat()
        )
        