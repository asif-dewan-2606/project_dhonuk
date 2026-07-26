import random
import uuid
from datetime import datetime, UTC

from config import MIN_CUSTOMER_ID, MAX_CUSTOMER_ID, MIN_MERCHANT_ID, MAX_MERCHANT_ID, MIN_AMOUNT, MAX_AMOUNT, STATUSES
from models import Transaction


class TransactionGenerator:

    def generate(self) -> Transaction:

        transaction = Transaction(
            transaction_id=str(uuid.uuid4()),
            customer_id=random.randint(
                MIN_CUSTOMER_ID,
                MAX_CUSTOMER_ID
            ),
            merchant_id=random.randint(
                MIN_MERCHANT_ID,
                MAX_MERCHANT_ID
            ),
            amount=round(
                random.uniform(
                    MIN_AMOUNT,
                    MAX_AMOUNT
                ),
                2
            ),
            status=random.choice(STATUSES),
            transaction_time = (
    datetime.now(UTC)
    .isoformat(timespec="milliseconds")
    .replace("+00:00", "Z")
)
        )

        return transaction