"""this script counts records in files(ndjson) of ozone bucket"""


import boto3

client = boto3.client(
    "s3",
    endpoint_url="http://ozone-s3g:9878",
    aws_access_key_id="ozone",
    aws_secret_access_key="ozone",
    region_name="us-east-1",
)

paginator = client.get_paginator("list_objects_v2")

records = 0

for page in paginator.paginate(Bucket="raw"):
    for obj in page.get("Contents", []):
        body = client.get_object(
            Bucket="raw",
            Key=obj["Key"]
        )["Body"].read().decode()

        records += len(body.splitlines())

print("Total records:", records)