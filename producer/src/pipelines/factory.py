from pipelines.pipeline import Pipeline

from publishers.factory import get_publisher

from sources.transaction_source import TransactionSource


def get_pipelines():

    return [

        Pipeline(
            name="transaction-generator",
            source=TransactionSource(),
            publisher=get_publisher("kafka")
        )

    ]