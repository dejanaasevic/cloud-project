import json
import boto3
import urllib.request
import urllib.parse
import os
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

s3 = boto3.client("s3")
BUCKET = os.environ["BRONZE_BUCKET"]
SEARCH_API = "https://hn.algolia.com/api/v1/search_by_date"
FIREBASE_USER_API = "https://hacker-news.firebaseio.com/v0/user/{}.json"
BATCH_SIZE = 1000
ITEM_TYPES = ["story", "comment", "ask_hn", "job", "poll"]
MAX_PUTS = int(os.environ.get("MAX_PUTS", 500))
USER_FETCH_WORKERS = int(os.environ.get("USER_FETCH_WORKERS", 20))


def fetch_json(url: str) -> dict | None:
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            return json.loads(resp.read())
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None


def fetch_pages(item_type: str, ts_start: int, ts_end: int):
    """Generator that yields one page of Algolia hits at a time."""
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

        yield result["hits"]

        if page >= nb_pages - 1:
            break

        page += 1


def fetch_user(username: str) -> dict | None:
    """Fetch a single user profile from the HN Firebase API."""
    url = FIREBASE_USER_API.format(urllib.parse.quote(username))
    data = fetch_json(url)
    if data:
        # 'submitted' lists every item ID the user ever posted —
        # can be thousands of entries, not needed here and bloats storage.
        data.pop("submitted", None)
    return data


def lambda_handler(event, context):
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")
    ts_start = int(yesterday.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    ts_end   = int(yesterday.replace(hour=23, minute=59, second=59, microsecond=0).timestamp())

    print(f"Collecting Hacker News data for: {date_str}")

    summary = {}
    put_counter = 0
    all_authors: set[str] = set()

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

            for item in page_hits:
                if item.get("author"):
                    all_authors.add(item["author"])

            key = f"{base_key}/hacker_news_{file_index}.json"
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
        print(f"  Total {item_type}: {total_items} items in {file_index} file(s)")

    # fetch user karma
    print(f"Fetching karma for {len(all_authors)} unique users...")
    users = []
    with ThreadPoolExecutor(max_workers=USER_FETCH_WORKERS) as executor:
        futures = {executor.submit(fetch_user, u): u for u in all_authors}
        for future in as_completed(futures):
            result = future.result()
            if result:
                users.append(result)

    print(f"  Fetched {len(users)}/{len(all_authors)} user profiles.")

    user_key = (
        f"hacker_news/users"
        f"/year={date_str[:4]}"
        f"/month={date_str[5:7]}"
        f"/day={date_str[8:]}"
        f"/users.json"
    )
    s3.put_object(
        Bucket=BUCKET,
        Key=user_key,
        Body=json.dumps(users, ensure_ascii=False),
        ContentType="application/json",
    )
    put_counter += 1
    print(f"  Written {len(users)} users -> {user_key} (PUT #{put_counter})")
    summary["users"] = len(users)

    print(f"Done. Summary: {summary}, total PUTs: {put_counter}")
    return {
        "statusCode": 200,
        "date": date_str,
        "summary": summary,
        "total_puts": put_counter,
        "limit_reached": False,
    }
