import os
import zipfile
import boto3
from datetime import date, timedelta
import botocore.exceptions
import pandas as pd
import json
import requests
import io
import random


def lambda_handler(event, context):

    """ Lambda function to check if the twitter file for yesterday already exists in the bronze bucket,
        if not it will load the dataset from kaggle, filter the tweets from yesterday and upload them as a json file to the s3 bucket."""

    # if the file alrady exists in the bronze bucket, skip the upload
    if already_exists():
        print("Twitter file already exists in the bronze bucket, no upload needed.")
        return
    
    # if the file doesn't exist, upload the yesterday's data from kaggle
    else:
        print("Twitter file does not exist in the bronze bucket, uploading yesterday's data.")

        # stream the dataset from kaggle in chunks
        df_chunks = stream_kaggle_csv_chunks()

        # try to get any data (random fallback)
        filtered_df = get_any_data(df_chunks)

        if filtered_df.empty:
            print("Still no data found after random attempts.")
        else:
            print(f"Found {len(filtered_df)} tweets")
        
        # upload tweets as a json file to the S3 bucket
        upload_as_json(filtered_df)
        

def already_exists() -> bool:

    """Check if the twitter file for yesterday already exists in the bronze bucket."""

    s3 = boto3.client("s3")

    # get bucket name from environment variable
    twitter_bucket_name = os.environ["BRONZE_TWITTER_BUCKET"]

    # calculate yesterday's date and  generate the key for the file in S3
    yesterday = date.today() - timedelta(days=1)
    key = f"twitter/year={yesterday.year}/month={yesterday.month:02d}/day={yesterday.day:02d}/tweets.json"

    print(f"Checking for file in S3://{twitter_bucket_name}/{key}")
    try:
        # check if the file exists in S3 bucket and return true if it does, otherwise return false
        s3.head_object(Bucket=twitter_bucket_name, Key=key)
        print(f"File exists in S3://{twitter_bucket_name}/{key}")
        return True
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print(f"File does not exist in S3://{twitter_bucket_name}/{key}")
            return False
        else:
            raise e


def get_any_data(df_chunks, attempts=10):
    all_chunks = list(df_chunks)

    dataset_start = date(2020, 7, 25)
    dataset_end = date(2020, 8, 30)
    total_days = (dataset_end - dataset_start).days

    for _ in range(attempts):
        random_day = dataset_start + timedelta(days=random.randint(0, total_days))

        print(f"Trying random date: {random_day}")

        result = filter_yesterday_tweets(all_chunks, random_day)
        if not result.empty:
            return result

    return pd.DataFrame()


def stream_kaggle_csv_chunks(chunk_size=10_000):

    """Stream the dataset from kaggle in chunks to avoid memory issues."""

    # kaggle api endpoint for downloading the dataset as a zip file
    url = "https://www.kaggle.com/api/v1/datasets/download/gpreda/covid19-tweets/covid19_tweets.csv"
    
    # get kaggle credentials from environment variables
    username = os.environ["KAGGLE_USERNAME"]
    key = os.environ["KAGGLE_KEY"]
    
    # stream the zip file from kaggle and read the csv in chunks without saving the full file to disk or holding it in memory
    with requests.get(url, auth=(username, key), stream=True) as r:
        # if the request was not successful, raise an error
        r.raise_for_status()
        
        # write the zip to /tmp in chunks and don't load the whole file in memory
        zip_path = "/tmp/covid_tweets.zip"
        with open(zip_path, "wb") as f:
            # take the response in 8MB chunks and write to file
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024): 
                f.write(chunk)
        
        # read the csv file from the zip in chunks using pandas and yield each chunk as a dataframe
        with zipfile.ZipFile(zip_path) as zf:
            with zf.open("covid19_tweets.csv") as csv_file:
                buffer = io.TextIOWrapper(csv_file, encoding="utf-8", errors="replace")
                # read the csv in chunks and yield each chunk as a dataframe
                for chunk in pd.read_csv(buffer, chunksize=chunk_size):
                    yield chunk


def filter_yesterday_tweets(df_chunks, date : date):

    """Filter the dataset to contain only tweets written on tthe date."""
    
    chunks = []

    for chunk in df_chunks:
        # convert the date column to datetime format and handle parsing errors
        chunk["date"] = pd.to_datetime(chunk["date"], format="mixed", utc=True, errors="coerce")
        # drop rows where date parsing failed and resulted in NaT, since we can't filter those rows
        chunk = chunk.dropna(subset=["date"]) 

        # create a filter to select only wanted rows
        chunk_filter = chunk["date"].dt.date == date

        # apply filter to the chunk and keep only yesterday's tweets
        filtered_chunk = chunk[chunk_filter]

        # if there are any data, collect it in list
        if not filtered_chunk.empty:
            chunks.append(filtered_chunk)

    # return empty data frame if there are not found any tweets, otherwise concatenate the chunks into a single dataframe and return it
    if not chunks:
        print("There is no tweet from yesterday in the dataset.")
        return pd.DataFrame()
    
    return pd.concat(chunks, ignore_index=True)


def upload_as_json(df: pd.DataFrame) -> None:

    """Upload the filtered tweet as a json file to the s3 bucket."""

    # get bucket name from environment variable
    twitter_bucket_name = os.environ.get("BRONZE_TWITTER_BUCKET", "")

    s3 = boto3.client("s3")

    # calculate yesterday's date
    yesterday = date.today() - timedelta(days=1)

    # generate the key for the file in S3
    key = f"twitter/year={yesterday.year}/month={yesterday.month:02d}/day={yesterday.day:02d}/tweets.json"

    # if no tweets found, upload an empty json file to S3 bucket to prevent the lambda from running again
    if df.empty:
        print("No tweets to upload, creating an empty file.")
        s3.put_object(Bucket=twitter_bucket_name, Key=key, Body=b"[]",ContentType="application/json")
        return

    # convert the dataframe to a list of records
    records = df.to_dict(orient="records")

    # convert the records to json format
    body = json.dumps(records, default=str).encode("utf-8")

    print(f"Uploading {len(records)} tweets to s3://{twitter_bucket_name}/{key}")

    # upload the data to S3
    s3.put_object(Bucket=twitter_bucket_name, Key=key, Body=body, ContentType="application/json")

    print("Upload completed")