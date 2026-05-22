import boto3
import os
from datetime import date, timedelta
import botocore.exceptions


def lambda_handler(event, context):
    s3 = boto3.client("s3")

    # get bucket name from environment variable
    twitter_bucket_name = os.environ["BRONZE_TWITTER_BUCKET"]

    # calculate yesterday's date and  generate the key for the file in S3
    yesterday = date.today() - timedelta(days=1)
    key = f"bronze/twitter/year={yesterday.year}/month={yesterday.month:02d}/day={yesterday.day:02d}/tweets.csv"

    print(f"Checking for file in s3://{twitter_bucket_name}/{key}")
    try:
        s3.head_object(Bucket=twitter_bucket_name, Key=key)
        print(f"File exists in s3://{twitter_bucket_name}/{key}")
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            raise FileNotFoundError(f"File doest not exists in s3://{twitter_bucket_name}/{key}")
        else:
            raise e