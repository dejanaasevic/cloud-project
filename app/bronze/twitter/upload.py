import os
import boto3
from datetime import date


def upload_csv_to_s3():
    twitter_file_path = "app/data/tweets.csv"
    twitter_bucket_name = os.environ.get("BRONZE_TWITTER_BUCKET", "")

    if not os.path.exists(twitter_file_path):
        print(f"File {twitter_file_path} does not exist. Please generate the tweets.csv file first.")
        return
    
    if not twitter_bucket_name:
        print("Set BRONZE_TWITTER_BUCKET env variable")
        return

    s3 = boto3.client("s3")
    today = date.today()

    # generate the key for the file in S3
    key = f"bronze/twitter/year={today.year}/month={today.month:02d}/day={today.day:02d}/tweets.csv"

    print(f"Uploading {twitter_file_path} to s3://{twitter_bucket_name}/{key}")

    # upload the file to S3
    s3.upload_file(Filename=twitter_file_path, Bucket=twitter_bucket_name, Key=key)

    print("Upload completed")

if __name__ == "__main__":
    upload_csv_to_s3()