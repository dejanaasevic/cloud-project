import os
from datetime import date, timedelta
import pandas as pd
import awswrangler as wr

TOP_N = 10


def lambda_handler(event, context):
    """Reads normalized silver data, computes gold layer metrics/KPIs, and writes parquet to the gold bucket."""

    if already_exists():
        print("Gold data already exists for this date, skipping.")
        return {"statusCode": 200, "message": "Already processed."}

    silver_bucket = os.environ["SILVER_BUCKET"]
    gold_bucket = os.environ["GOLD_BUCKET"]
    yesterday = date.today() - timedelta(days=1)

    hn_posts = load_posts(silver_bucket, yesterday, "hacker_news_")
    tw_posts = load_posts(silver_bucket, yesterday, "twitter_")
    hn_users = load_users(silver_bucket, "HackerNews")
    tw_users = load_users(silver_bucket, "twitter")

    write_daily_post_type_metric(gold_bucket, yesterday, hn_posts)
    write_daily_users_metric(gold_bucket, yesterday, hn_posts, tw_posts)
    write_top_twitter_users_by_followers(gold_bucket, yesterday, tw_users)
    write_top_hn_users_by_karma(gold_bucket, yesterday, hn_users)
    write_top_hn_posts_by_score(gold_bucket, yesterday, hn_posts, "job", "top_hn_jobs_by_score")
    write_top_hn_posts_by_score(gold_bucket, yesterday, hn_posts, "story", "top_hn_stories_by_score")
    write_data_quality_score(
        gold_bucket,
        yesterday,
        {
            "posts_hacker_news": hn_posts,
            "posts_twitter": tw_posts,
            "users_hacker_news": hn_users,
            "users_twitter": tw_users,
        },
    )

    print("Gold layer processing completed.")
    return {"statusCode": 200, "message": "Gold metrics computed."}


def already_exists() -> bool:
    """Check if gold metrics for yesterday already exist."""
    gold_bucket = os.environ["GOLD_BUCKET"]
    yesterday = date.today() - timedelta(days=1)
    path = f"s3://{gold_bucket}/daily_users_metric/platform=HackerNews/date={yesterday.isoformat()}/"

    existing = wr.s3.list_objects(path)
    if any("gold_" in f for f in existing):
        print(f"Gold data already exists at {path}, skipping.")
        return True

    print(f"No gold data found at {path}, processing.")
    return False


def load_posts(silver_bucket: str, yesterday: date, filename_prefix: str) -> pd.DataFrame:
    """Load yesterday's silver posts partition for a single platform, keyed by its filename prefix."""
    prefix_path = (
        f"s3://{silver_bucket}/posts/"
        f"year={yesterday.year}/month={yesterday.month:02d}/day={yesterday.day:02d}/"
    )
    try:
        all_files = wr.s3.list_objects(prefix_path)
    except Exception:
        all_files = []

    matching = [f for f in all_files if filename_prefix in f]
    if not matching:
        print(f"No posts found at {prefix_path} matching '{filename_prefix}'.")
        return pd.DataFrame()

    return wr.s3.read_parquet(path=matching)


def load_users(silver_bucket: str, platform: str) -> pd.DataFrame:
    """Load the silver users table for a platform, collapsed to one (latest known) row per username.

    The silver layer appends daily snapshots without cross-run dedup, so the same
    username can appear multiple times across different ingestion days.
    """
    path = f"s3://{silver_bucket}/users/platform={platform}/"
    try:
        df = wr.s3.read_parquet(path=path, dataset=True)
    except Exception as e:
        print(f"No users found at {path}: {e}")
        return pd.DataFrame()

    if df.empty:
        return df

    rank_col = "karma_score" if platform == "HackerNews" else "followers_count"
    return df.sort_values(rank_col, ascending=False).drop_duplicates(subset=["username"], keep="first")


def write_gold_table(gold_bucket: str, table_name: str, df: pd.DataFrame, partition_cols: list):
    """Append a gold-layer dataframe to its partitioned parquet table."""
    path = f"s3://{gold_bucket}/{table_name}/"
    print(f"Writing {len(df)} rows to {path}")
    wr.s3.to_parquet(
        df=df,
        path=path,
        dataset=True,
        partition_cols=partition_cols,
        mode="append",
        filename_prefix="gold_",
    )


def write_daily_post_type_metric(gold_bucket: str, yesterday: date, hn_posts: pd.DataFrame):
    """Daily count of HN items per post_type (story, ask, comment, job, poll)."""
    if hn_posts.empty:
        print("No HN posts found, skipping daily_post_type_metric.")
        return

    counts = hn_posts.groupby("post_type").size().reset_index(name="post_count")
    counts["platform"] = "HackerNews"
    counts["date"] = yesterday.isoformat()
    write_gold_table(gold_bucket, "daily_post_type_metric", counts, partition_cols=["date"])

def write_daily_users_metric(gold_bucket: str, yesterday: date, hn_posts: pd.DataFrame, tw_posts: pd.DataFrame):
    """Daily count of distinct users who posted/tweeted, per platform."""
    rows = []
    for platform, posts in [("HackerNews", hn_posts), ("twitter", tw_posts)]:
        total_users = 0 if posts.empty else posts["author_username"].nunique()
        rows.append({"platform": platform, "date": yesterday.isoformat(), "total_users": total_users})

    out_df = pd.DataFrame(rows)
    write_gold_table(gold_bucket, "daily_users_metric", out_df, partition_cols=["platform", "date"])


def write_top_twitter_users_by_followers(gold_bucket: str, yesterday: date, tw_users: pd.DataFrame):
    """Top 10 twitter users by follower count."""
    if tw_users.empty:
        print("No twitter users found, skipping top_twitter_users_by_followers.")
        return

    ranked = tw_users.dropna(subset=["followers_count"])
    if ranked.empty:
        print("No twitter users with follower counts, skipping top_twitter_users_by_followers.")
        return

    top = ranked.nlargest(TOP_N, "followers_count")[["username", "followers_count"]].reset_index(drop=True)
    top["rank"] = top.index + 1
    top["date"] = yesterday.isoformat()
    write_gold_table(gold_bucket, "top_twitter_users_by_followers", top, partition_cols=["date"])


def write_top_hn_users_by_karma(gold_bucket: str, yesterday: date, hn_users: pd.DataFrame):
    """Top 10 HN users with the highest and lowest karma_score."""
    if hn_users.empty:
        print("No HN users found, skipping top_hn_users_by_karma.")
        return

    ranked = hn_users.dropna(subset=["karma_score"])
    if ranked.empty:
        print("No HN users with karma scores, skipping top_hn_users_by_karma.")
        return

    top_high = ranked.nlargest(TOP_N, "karma_score")[["username", "karma_score"]].reset_index(drop=True)
    top_high["rank"] = top_high.index + 1
    top_high["date"] = yesterday.isoformat()
    write_gold_table(gold_bucket, "top_hn_users_by_karma_high", top_high, partition_cols=["date"])

    top_low = ranked.nsmallest(TOP_N, "karma_score")[["username", "karma_score"]].reset_index(drop=True)
    top_low["rank"] = top_low.index + 1
    top_low["date"] = yesterday.isoformat()
    write_gold_table(gold_bucket, "top_hn_users_by_karma_low", top_low, partition_cols=["date"])


def write_top_hn_posts_by_score(gold_bucket: str, yesterday: date, hn_posts: pd.DataFrame, post_type: str, table_name: str):
    """Top 10 HN items of a given post_type (job or story) with the highest score."""
    if hn_posts.empty:
        print(f"No HN posts found, skipping {table_name}.")
        return

    ranked = hn_posts[hn_posts["post_type"] == post_type].dropna(subset=["score"])
    if ranked.empty:
        print(f"No HN '{post_type}' posts with a score, skipping {table_name}.")
        return

    top = ranked.nlargest(TOP_N, "score")[["post_id", "title", "score"]].reset_index(drop=True)
    top["rank"] = top.index + 1
    top["date"] = yesterday.isoformat()
    write_gold_table(gold_bucket, table_name, top, partition_cols=["date"])


def write_data_quality_score(gold_bucket: str, yesterday: date, tables: dict):
    """KPI: percentage of non-null cells per silver table, indicating normalization quality."""
    rows = []
    for name, df in tables.items():
        if df.empty:
            rows.append({"table_name": name, "row_count": 0, "quality_score": None})
            continue

        total_cells = df.size
        non_null_cells = int(df.notna().sum().sum())
        quality_score = round(non_null_cells / total_cells * 100, 2)
        rows.append({"table_name": name, "row_count": len(df), "quality_score": quality_score})

    out_df = pd.DataFrame(rows)
    out_df["date"] = yesterday.isoformat()
    write_gold_table(gold_bucket, "data_quality_score", out_df, partition_cols=["date"])
