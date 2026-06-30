import json
import uuid
import html
import re
import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timezone, date, timedelta
import pandas as pd
import awswrangler as wr

ITEM_TYPES = ["story", "comment", "ask_hn", "job", "poll"]


def lambda_handler(event, context):
    """Reads raw Hacker News JSON from bronze bucket, normalizes it, and writes parquet to silver bucket."""

    if already_exists():
        print("Silver data already exists for this date, skipping.")
        return {"statusCode": 200, "message": "Already processed."}

    bronze_bucket = os.environ["BRONZE_HN_BUCKET"]
    silver_bucket = os.environ["SILVER_HN_BUCKET"]

    yesterday = date.today() - timedelta(days=1)
    s3_client = boto3.client("s3")

    user_karma = load_user_karma(s3_client, bronze_bucket, yesterday)

    all_items = []
    for item_type in ITEM_TYPES:
        prefix = (
            f"hacker_news/{item_type}"
            f"/year={yesterday.year}"
            f"/month={yesterday.month:02d}"
            f"/day={yesterday.day:02d}/"
        )
        path = f"s3://{bronze_bucket}/{prefix}"

        try:
            file_paths = wr.s3.list_objects(path)
        except Exception:
            file_paths = []

        for s3_path in file_paths:
            key = s3_path.replace(f"s3://{bronze_bucket}/", "")
            response = s3_client.get_object(Bucket=bronze_bucket, Key=key)
            items = json.loads(response["Body"].read())
            if isinstance(items, list):
                all_items.extend(items)
                print(f"  Loaded {len(items)} items from {key}")

    if not all_items:
        print("No items found in bronze, skipping.")
        return {"statusCode": 200, "message": "No data to process."}

    print(f"Total items loaded: {len(all_items)}")

    users = []
    posts = []
    for item in all_items:
        users.append(parse_user(item, user_karma))
        posts.append(parse_post(item))

    users_df = pd.DataFrame(users)
    posts_df = pd.DataFrame(posts)

    users_df["karma_score"] = users_df["karma_score"].astype("Int64")
    posts_df["score"] = posts_df["score"].astype("Int64")

    users_df = users_df.drop_duplicates(subset=["username"])
    posts_df = posts_df.drop_duplicates(subset=["post_id"])

    # partition by the actual HN item creation date (always yesterday)
    posts_df["year"]  = posts_df["created_at"].str[:4]
    posts_df["month"] = posts_df["created_at"].str[5:7]
    posts_df["day"]   = posts_df["created_at"].str[8:10]

    print(f"Writing {len(posts_df)} posts and {len(users_df)} users to silver bucket.")

    wr.s3.to_parquet(
        df=posts_df,
        path=f"s3://{silver_bucket}/posts/",
        dataset=True,
        partition_cols=["year", "month", "day"],
        mode="append",
    )

    wr.s3.to_parquet(
        df=users_df,
        path=f"s3://{silver_bucket}/users/",
        dataset=True,
        partition_cols=["platform"],
        mode="append",
    )

    print("Silver HN processing completed.")
    return {
        "statusCode": 200,
        "message": f"Processed {len(posts_df)} posts and {len(users_df)} users.",
    }


def load_user_karma(s3_client, bronze_bucket: str, yesterday: date) -> dict:
    """Load username -> karma mapping from the bronze users file."""
    key = (
        f"hacker_news/users"
        f"/year={yesterday.year}"
        f"/month={yesterday.month:02d}"
        f"/day={yesterday.day:02d}"
        f"/users.json"
    )
    try:
        response = s3_client.get_object(Bucket=bronze_bucket, Key=key)
        raw_users = json.loads(response["Body"].read())
        karma_map = {u["id"]: u.get("karma") for u in raw_users if u.get("id")}
        print(f"Loaded karma for {len(karma_map)} users.")
        return karma_map
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            print("No user karma file found in bronze, karma_score will be null.")
        else:
            print(f"Error loading user karma: {e}")
        return {}


def already_exists() -> bool:
    """Check if silver posts for yesterday already exist."""
    silver_bucket = os.environ["SILVER_HN_BUCKET"]
    yesterday = date.today() - timedelta(days=1)
    path = (
        f"s3://{silver_bucket}/posts/"
        f"year={yesterday.year}/month={yesterday.month:02d}/day={yesterday.day:02d}/"
    )

    existing = wr.s3.list_objects(path)
    if existing:
        print(f"Silver data already exists at {path}, skipping.")
        return True

    print(f"No silver data found at {path}, processing.")
    return False


def get_post_type(tags: list) -> str:
    """Map HN _tags to a normalized post type."""
    if "comment" in tags:
        return "comment"
    if "job" in tags:
        return "job"
    if "poll" in tags:
        return "poll"
    if "ask_hn" in tags:
        return "ask"
    return "story"


def clean_html(text: str) -> str:
    """Strip HTML tags and unescape HTML entities from HN content."""
    if not text:
        return text
    text = re.sub(r"<p\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    return text.strip()


def normalize_timestamp(raw_date: str) -> str:
    """Convert a date string to UTC ISO-8601 format, handling Z suffix."""
    if not raw_date:
        return None
    raw_date = raw_date.replace("Z", "+00:00")
    dt = datetime.fromisoformat(raw_date)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_user(item: dict, user_karma: dict) -> dict:
    """Extract and normalize user data from a raw HN item."""
    username = item.get("author", "")
    return {
        "user_id": str(uuid.uuid4()),
        "username": username,
        "platform": "HackerNews",
        "karma_score": user_karma.get(username),
        "is_verified": None,
        "created_at": None,
    }


def parse_post(item: dict) -> dict:
    """Extract and normalize post data from a raw HN item."""
    tags = item.get("_tags", [])

    raw_text = item.get("story_text") or item.get("comment_text") or ""
    title = item.get("title") or ""

    content_text = clean_html(raw_text) if raw_text else clean_html(title)

    return {
        "post_id": str(item.get("objectID", "")),
        "author_username": item.get("author", ""),
        "title": clean_html(title),
        "content_text": content_text,
        "created_at": normalize_timestamp(item.get("created_at", "")),
        "post_type": get_post_type(tags),
        "score": item.get("points"),
        "parent_id": str(item["parent_id"]) if item.get("parent_id") else None,
        "source_platform": "HackerNews",
    }
