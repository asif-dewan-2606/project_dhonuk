import random
import time
import uuid
from datetime import UTC, datetime

from models import Transaction
from sources.base import Source


RESPONSE_CODES = [
    "00",
    "05",
    "51",
    "96",
]

STATUS = [
    "S",
    "F",
]

TXN_TYPES = [
    "CW",
    "SM",
    "AP",
    None,
]

PROCESSING_CODES = [
    "010000",
    "280000",
    "400000",
]

TXN_TYPE_DC = [
    "D",
    "C",
    None,
]

TXN_CATEGORIES = [
    "P2P",
    "MPAY",
    "CASH",
    None,
]

POS_ENTRY_MODES = [
    "01",
    "02",
    "05",
    None,
]

CUSTOMER_SEGMENTS = [
    "R",
    "P",
    None,
]

ACCOUNT_TYPES = [
    "SA",
    "WA",
    None,
]

TXN_SUB_TYPES = [
    "NORMAL",
    "REVERSAL",
    None,
]


class TransactionSource(Source):

    def __init__(self, events_per_second: int = 10):
        if events_per_second <= 0:
            raise ValueError(
                "events_per_second must be greater than zero."
            )

        self.interval = 1 / events_per_second

    def fetch(self):
        while True:
            yield self._generate_transaction()
            time.sleep(self.interval)

    def _generate_transaction(self) -> Transaction:

        now = datetime.now(UTC)

        return Transaction(
            id=random.randint(1, 10_000_000_000),
            sqn=random.randint(1, 99_999),

            approval_date=int(now.strftime("%Y%m%d")),
            approval_datetime=now,

            nr_number=str(uuid.uuid4())[:16],

            response_code=random.choice(RESPONSE_CODES),
            status=random.choice(STATUS),

            txn_type=random.choice(TXN_TYPES),
            processing_code=random.choice(PROCESSING_CODES),

            txn_type_d_c=random.choice(TXN_TYPE_DC),
            txn_cat=random.choice(TXN_CATEGORIES),
            pos_entry_mode=random.choice(POS_ENTRY_MODES),

            par=f"016{random.randint(10000000, 99999999)}",

            target_par=random.choice([
                f"017{random.randint(10000000, 99999999)}",
                None,
            ]),

            txn_amt=round(random.uniform(10, 10000), 2),

            acc_blc=round(
                random.uniform(1000, 100000),
                2,
            ),

            acc_available_blc=round(
                random.uniform(500, 90000),
                2,
            ),

            user_id=f"user{random.randint(1000, 9999)}",

            customer_segment=random.choice(
                CUSTOMER_SEGMENTS
            ),

            trust_level=random.randint(1, 5),

            target_user_id=f"user{random.randint(1000, 9999)}",

            target_customer_segment=random.choice(
                CUSTOMER_SEGMENTS
            ),

            target_trust_level=random.randint(1, 5),

            target_account_type=random.choice(
                ACCOUNT_TYPES
            ),

            account_id1=str(uuid.uuid4())[:20],

            created=now,
            updated=now,

            txn_id=f"{random.randint(0, 999999999999):012}",

            account_type=random.choice(
                ACCOUNT_TYPES
            ),

            txn_sub_type=random.choice(
                TXN_SUB_TYPES
            ),

            org_nr_number=str(uuid.uuid4())[:16],

            sync_flag=random.choice([0, 1]),

            rrnum=f"{random.randint(0, 999999999999):012}",

            stan=random.randint(
                1,
                999999999999999999,
            ),
        )