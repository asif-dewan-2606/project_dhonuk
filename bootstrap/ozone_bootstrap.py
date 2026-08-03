import os
import time
import boto3
from botocore.exceptions import ClientError, EndpointConnectionError

REQUIRED_BUCKETS = [
    "raw",
    "bronze",
    "silver",
    "gold",
    "platinum",
]

RETRY_SECONDS = 5
MAX_RETRIES = 24  # 2 minutes


def get_client():
    return boto3.client(
        service_name="s3",
        endpoint_url=os.environ["OZONE_ENDPOINT"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name="us-east-1",
    )


def wait_for_s3(client):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            client.list_buckets()
            print("✓ Ozone S3 Gateway is available.")
            return
        except EndpointConnectionError:
            print(
                f"[{attempt}/{MAX_RETRIES}] Waiting for Ozone S3 Gateway..."
            )
            time.sleep(RETRY_SECONDS)

    raise RuntimeError("Timed out waiting for Ozone S3 Gateway.")


def ensure_buckets(client):
    response = client.list_buckets()

    existing = {
        bucket["Name"]
        for bucket in response.get("Buckets", [])
    }

    for bucket in REQUIRED_BUCKETS:
        if bucket in existing:
            print(f"✓ Bucket already exists: {bucket}")
            continue

        print(f"Creating bucket: {bucket}")

        try:
            client.create_bucket(Bucket=bucket)
            print(f"✓ Bucket created: {bucket}")

        except ClientError as ex:
            print(f"✗ Failed creating bucket {bucket}")
            raise ex


def main():
    print("Starting Ozone bootstrap...")

    client = get_client()

    wait_for_s3(client)

    ensure_buckets(client)

    print("Bootstrap completed successfully.")


if __name__ == "__main__":
    main()