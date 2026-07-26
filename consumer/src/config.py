import os

# ------------------------
# Kafka
# ------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sales_transactions")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "sales-consumer-group")

# ------------------------
# Batch
# ------------------------

BATCH_SIZE = 500
BATCH_TIMEOUT_MS = 100

# ------------------------
# ClickHouse
# ------------------------

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))

CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "analytics")
CLICKHOUSE_TABLE = os.getenv("CLICKHOUSE_TABLE", "sales_transactions")

CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "dhonuk")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "dhonuk123")