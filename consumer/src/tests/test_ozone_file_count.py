import boto3
import os

# client = boto3.client(
#     service_name="s3",
#     endpoint_url="http://ozone-s3g:9878",
#     aws_access_key_id="ozone",
#     aws_secret_access_key="ozone",
#     region_name="us-east-1",
# )

# response = client.list_buckets()

# print(response)




# client = boto3.client(
#     "s3",
#     endpoint_url=os.environ["OZONE_ENDPOINT"],
#     aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
#     aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
#     region_name="us-east-1",
# )

# client.put_object(
#     Bucket="raw",
#     Key="test.txt",
#     Body=b"hello world"
# )

## counting files in ozone bucket ##

import boto3

client = boto3.client(
    "s3",
    endpoint_url="http://ozone-s3g:9878",
    aws_access_key_id="ozone",
    aws_secret_access_key="ozone",
    region_name="us-east-1",
)

paginator = client.get_paginator("list_objects_v2")

count = 0

for page in paginator.paginate(Bucket="raw"):
    for obj in page.get("Contents", []):
        count += 1
        print(obj["Key"])

print(f"\nTotal files: {count}")