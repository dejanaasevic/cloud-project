import json
import boto3
import urllib.request
import urllib.parse
import os
from datetime import datetime, timedelta, timezone

s3 = boto3.client("s3")
BUCKET = os.environ["BRONZE_BUCKET"]
SEARCH_API = "https://hn.algolia.com/api/v1/search_by_date"
BATCH_SIZE = 1000
ITEM_TYPES = ["story", "comment", "ask_hn", "job", "poll"]
MAX_PUTS = int(os.environ.get("MAX_PUTS", 500))  # 0 for unlimited


def fetch_json(url: str) -> dict | None:
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            return json.loads(resp.read())
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None


def fetch_pages(item_type: str, ts_start: int, ts_end: int):
    """Generator that yields one page per call."""
    page = 0

    while True:
        params = urllib.parse.urlencode({
            "tags": item_type,
            "numericFilters": f"created_at_i>{ts_start},created_at_i<{ts_end}",
            "hitsPerPage": BATCH_SIZE,
            "page": page,
        })
        result = fetch_json(f"{SEARCH_API}?{params}")

        if not result or not result.get("hits"):
            break

        nb_pages = result.get("nbPages", 1)
        print(f"  [{item_type}] page {page + 1}/{nb_pages} — {len(result['hits'])} items")

        yield result["hits"]  # yield the page, pause until next call

        if page >= nb_pages - 1:
            break

        page += 1


def lambda_handler(event, context):
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")
    ts_start = int(yesterday.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    ts_end   = int(yesterday.replace(hour=23, minute=59, second=59, microsecond=0).timestamp())

    print(f"Collecting Hacker News data for: {date_str}")

    summary = {}
    put_counter = 0  # list so it can be mutated inside nested functions

    for item_type in ITEM_TYPES:
        print(f"Fetching: {item_type}")
        total_items = 0
        file_index = 0

        base_key = (
            f"hacker_news/{item_type}"
            f"/year={date_str[:4]}"
            f"/month={date_str[5:7]}"
            f"/day={date_str[8:]}"
        )

        for page_hits in fetch_pages(item_type, ts_start, ts_end):
            # Check limit before each PUT
            if MAX_PUTS and put_counter >= MAX_PUTS:
                print(f"Reached limit of {MAX_PUTS} PUT requests, stopping.")
                summary[item_type] = total_items
                return {
                    "statusCode": 200,
                    "date": date_str,
                    "summary": summary,
                    "total_puts": put_counter,
                    "limit_reached": True,
                }

            key = f"{base_key}/data_{file_index}.json"
            s3.put_object(
                Bucket=BUCKET,
                Key=key,
                Body=json.dumps(page_hits, ensure_ascii=False),
                ContentType="application/json",
            )
            put_counter += 1
            file_index += 1
            total_items += len(page_hits)
            print(f"  Written {len(page_hits)} items -> {key} (PUT #{put_counter})")

        summary[item_type] = total_items
        print(f"  Total: {total_items} items in {file_index} file(s)")

    print(f"Done. Summary: {summary}, total PUTs: {put_counter}")
    return {
        "statusCode": 200,
        "date": date_str,
        "summary": summary,
        "total_puts": put_counter,
        "limit_reached": False,
    }