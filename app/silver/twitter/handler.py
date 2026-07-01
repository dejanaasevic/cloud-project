import json
import uuid
import hashlib
import os
import boto3
from datetime import datetime, timezone, date, timedelta
import pandas as pd
import awswrangler as wr


def lambda_handler(event, context):
    """Reads raw twitter JSON from bronze bucket, normalizes it, and writes parquet to silver bucket."""

    if already_exists():
        print("Silver data already exists for this date, skipping.")
        return {"statusCode": 200, "message": "Already processed."}

    bronze_bucket = os.environ["BRONZE_TWITTER_BUCKET"]

    yesterday = date.today() - timedelta(days=1)
    key = f"twitter/year={yesterday.year}/month={yesterday.month:02d}/day={yesterday.day:02d}/tweets.json"
    
    print(f"Reading bronze file: s3://{bronze_bucket}/{key}")
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bronze_bucket, Key=key)
    data = json.loads(response["Body"].read())

    if not data:
        print("No tweets found in bronze file, skipping.")
        return {"statusCode": 200, "message": "No data to process."}

    print(f"Found {len(data)} tweets to process.")

    users = []
    posts = []
    for tweet in data:
        users.append(parse_user(tweet))
        posts.append(parse_post(tweet))

    users_df = pd.DataFrame(users)
    posts_df = pd.DataFrame(posts)

    # cast nullable integer columns explicitly
    users_df["karma_score"] = users_df["karma_score"].astype("Int64")
    posts_df["score"] = posts_df["score"].astype("Int64")

    # remove duplicates
    users_df = users_df.drop_duplicates(subset=["username"])
    posts_df = posts_df.drop_duplicates(subset=["post_id"])

    # add partition columns extracted from created_at timestamp
    posts_df["year"]  = posts_df["created_at"].str[:4]
    posts_df["month"] = posts_df["created_at"].str[5:7]
    posts_df["day"]   = posts_df["created_at"].str[8:10]

    print(f"Writing {len(posts_df)} posts and {len(users_df)} users to silver bucket.")

    silver_bucket = os.environ["SILVER_TWITTER_BUCKET"]

    posts_path = f"s3://{silver_bucket}/posts/"
    print(f"Writing posts to: {posts_path}")
    wr.s3.to_parquet(df=posts_df, path=posts_path, dataset=True, partition_cols=["year", "month", "day"], mode="append", filename_prefix="twitter_")

    users_path = f"s3://{silver_bucket}/users/"
    print(f"Writing users to: {users_path}")
    wr.s3.to_parquet(df=users_df, path=users_path, dataset=True, partition_cols=["platform"], mode="append")

    print("Silver layer processing completed.")
    return {"statusCode": 200, "message": f"Processed {len(posts_df)} posts and {len(users_df)} users."}


def already_exists() -> bool:
    """Check if silver posts for the processing date already exist."""

    silver_bucket = os.environ["SILVER_TWITTER_BUCKET"]

    yesterday = date.today() - timedelta(days=1)
    path = f"s3://{silver_bucket}/posts/year={yesterday.year}/month={yesterday.month:02d}/day={yesterday.day:02d}/"

    existing = wr.s3.list_objects(path)
    if any("twitter_" in f for f in existing):
        print(f"Silver data already exists at {path}, skipping.")
        return True

    print(f"No silver data found at {path}, processing.")
    return False


def parse_user(tweet: dict) -> dict:
    """Extract and normalize user data from a raw tweet record."""

    user = {}
    user["user_id"] = str(uuid.uuid4())
    user["username"] = tweet["user_name"]
    user["platform"] = "twitter"
    user["karma_score"] = None
    user["is_verified"] = tweet["user_verified"]
    user["followers_count"] = tweet["user_followers"]
    user["created_at"] = convert_to_utc(tweet["user_created"])
    return user


def parse_post(tweet: dict) -> dict:
    """Extract and normalize post data from a raw tweet record."""

    post = {}
    post["post_id"] = hashlib.md5(f"{tweet['user_name']}{convert_to_utc(tweet['date'])}{tweet['text']}".encode()).hexdigest()
    post["author_username"] = tweet["user_name"]
    post["content_text"] = tweet["text"]
    post["created_at"] = convert_to_utc(tweet["date"])
    post["post_type"] = "retweet" if tweet.get("is_retweet") else "tweet"
    post["score"] = None
    post["parent_id"] = None
    post["source_platform"] = "twitter"
    return post


def convert_to_utc(raw_date: str) -> str:
    """Convert a date string to UTC ISO-8601 format"""

    dt = datetime.fromisoformat(raw_date)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")