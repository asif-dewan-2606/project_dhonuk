import random
import uuid
from datetime import datetime, UTC

from models import Transaction


class TransactionGenerator:

    def generate(self) -> Transaction:
        now = datetime.now(UTC)

        return Transaction(
            id=random.randint(1, 10_000_000_000),
            sqn=random.randint(1, 99999),
            approval_date=int(now.strftime("%Y%m%d")),
            approval_datetime=now,
            nr_number=str(uuid.uuid4())[:16],
            response_code=random.choice(["00", "05", "51", "96"]),
            status=random.choice(["S", "F"]),
            txn_type=random.choice(["CW", "SM", "AP", None]),
            processing_code=random.choice(["010000", "280000", "400000"]),
            txn_type_d_c=random.choice(["D", "C", None]),
            txn_cat=random.choice(["P2P", "MPAY", "CASH", None]),
            pos_entry_mode=random.choice(["01", "02", "05", None]),
            par=f"016{random.randint(10000000, 99999999)}",
            target_par=random.choice([
                f"017{random.randint(10000000, 99999999)}",
                None,
            ]),
            txn_amt=round(random.uniform(10, 10000), 2),
            acc_blc=round(random.uniform(1000, 100000), 2),
            acc_available_blc=round(random.uniform(500, 90000), 2),
            user_id=f"user{random.randint(1000,9999)}",
            customer_segment=random.choice(["R", "P", None]),
            trust_level=random.randint(1, 5),
            target_user_id=f"user{random.randint(1000,9999)}",
            target_customer_segment=random.choice(["R", "P", None]),
            target_trust_level=random.randint(1, 5),
            target_account_type=random.choice(["SA", "WA", None]),
            account_id1=str(uuid.uuid4())[:20],
            created=now,
            updated=now,
            txn_id=f"{random.randint(0, 999999999999):012}",
            account_type=random.choice(["SA", "WA", None]),
            txn_sub_type=random.choice(["NORMAL", "REVERSAL", None]),
            org_nr_number=str(uuid.uuid4())[:16],
            sync_flag=random.choice([0, 1]),
            rrnum=f"{random.randint(0, 999999999999):012}",
            stan=random.randint(1, 999999999999999999),
        )
