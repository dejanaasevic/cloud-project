import json
import os
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from urllib.parse import unquote

import awswrangler as wr
import boto3
import pandas as pd
import pg8000.dbapi


BATCH_SIZE = 1000
DATE_PARTITION_PATTERN = re.compile(
    r"(?:^|/)date=(\d{4}-\d{2}-\d{2})(?:/|$)"
)



GOLD_TABLES = {
    "daily_post_type_metric": {
        "columns": {
            "post_type": "TEXT NOT NULL",
            "post_count": "BIGINT NOT NULL",
            "platform": "TEXT NOT NULL",
            "date": "DATE NOT NULL",
        },
        "conflict_columns": [
            "post_type",
            "platform",
            "date",
        ],
    },
    "daily_users_metric": {
        "columns": {
            "platform": "TEXT NOT NULL",
            "date": "DATE NOT NULL",
            "total_users": "BIGINT NOT NULL",
        },
        "conflict_columns": [
            "platform",
            "date",
        ],
    },
    "top_twitter_users_by_followers": {
        "columns": {
            "username": "TEXT NOT NULL",
            "followers_count": "BIGINT NOT NULL",
            "rank": "INTEGER NOT NULL",
            "date": "DATE NOT NULL",
        },
        "conflict_columns": [
            "username",
            "date",
        ],
    },
    "top_hn_users_by_karma_high": {
        "columns": {
            "username": "TEXT NOT NULL",
            "karma_score": "BIGINT NOT NULL",
            "rank": "INTEGER NOT NULL",
            "date": "DATE NOT NULL",
        },
        "conflict_columns": [
            "username",
            "date",
        ],
    },
    "top_hn_users_by_karma_low": {
        "columns": {
            "username": "TEXT NOT NULL",
            "karma_score": "BIGINT NOT NULL",
            "rank": "INTEGER NOT NULL",
            "date": "DATE NOT NULL",
        },
        "conflict_columns": [
            "username",
            "date",
        ],
    },
    "top_hn_jobs_by_score": {
        "columns": {
            "post_id": "TEXT NOT NULL",
            "title": "TEXT",
            "score": "BIGINT NOT NULL",
            "rank": "INTEGER NOT NULL",
            "date": "DATE NOT NULL",
        },
        "conflict_columns": [
            "post_id",
            "date",
        ],
    },
    "top_hn_stories_by_score": {
        "columns": {
            "post_id": "TEXT NOT NULL",
            "title": "TEXT",
            "score": "BIGINT NOT NULL",
            "rank": "INTEGER NOT NULL",
            "date": "DATE NOT NULL",
        },
        "conflict_columns": [
            "post_id",
            "date",
        ],
    },
    "data_quality_score": {
        "columns": {
            "table_name": "TEXT NOT NULL",
            "row_count": "BIGINT NOT NULL",
            "quality_score": "DOUBLE PRECISION",
            "date": "DATE NOT NULL",
        },
        "conflict_columns": [
            "table_name",
            "date",
        ],
    },
}


def lambda_handler(event, context):
    """
    Load Gold Parquet metrics into PostgreSQL.

    Supported modes:

    incremental:
        Processes only dates that have not previously been processed.
        The last processed date is processed again to support corrections.

    full:
        Processes every available Gold partition.
    """

    mode = str(
        (event or {}).get("mode", "incremental")
    ).strip().lower()

    if mode not in {"incremental", "full"}:
        raise ValueError(
            "Supported modes are 'incremental' and 'full'."
        )

    connection = None

    try:
        gold_bucket = require_environment_variable(
            "GOLD_BUCKET"
        )

        connection = create_database_connection()

        create_sync_state_table(connection)

        processed_tables: dict[str, dict[str, Any]] = {}
        total_rows = 0

        for table_name, table_config in GOLD_TABLES.items():
            table_result = synchronize_table(
                connection=connection,
                gold_bucket=gold_bucket,
                table_name=table_name,
                table_config=table_config,
                mode=mode,
            )

            processed_tables[table_name] = table_result
            total_rows += table_result["rows"]

        connection.commit()

        result = {
            "message": (
                "Gold data successfully synchronized "
                "with PostgreSQL."
            ),
            "mode": mode,
            "processed_tables": processed_tables,
            "total_rows": total_rows,
        }

        print(json.dumps(result, default=str))

        return {
            "statusCode": 200,
            "body": json.dumps(
                result,
                default=str,
            ),
        }

    except Exception as error:
        if connection is not None:
            connection.rollback()

        print(
            "Gold-to-PostgreSQL synchronization failed: "
            f"{error}"
        )

        raise

    finally:
        if connection is not None:
            connection.close()


def create_database_connection():
    """
    Create a PostgreSQL connection using credentials
    stored in AWS Secrets Manager.
    """

    host = require_environment_variable("DB_HOST")
    port = int(os.environ.get("DB_PORT", "5432"))

    secret_arn = require_environment_variable(
        "DB_SECRET_ARN"
    )

    secrets_client = boto3.client(
        "secretsmanager"
    )

    secret_response = secrets_client.get_secret_value(
        SecretId=secret_arn,
    )

    secret_data = json.loads(
        secret_response["SecretString"]
    )

    print(
        f"Connecting to PostgreSQL at {host}:{port}/"
        f"{secret_data['database']}"
    )

    return pg8000.dbapi.connect(
        host=host,
        port=port,
        database=secret_data["database"],
        user=secret_data["username"],
        password=secret_data["password"],
        timeout=30,
    )


def create_sync_state_table(connection):
    """
    Create the internal watermark table used by
    incremental synchronization.
    """

    sql = """
        CREATE TABLE IF NOT EXISTS visualization_sync_state (
            table_name TEXT PRIMARY KEY,
            last_processed_date DATE NOT NULL,
            updated_at TIMESTAMPTZ
                NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """

    cursor = connection.cursor()

    try:
        cursor.execute(sql)
    finally:
        cursor.close()


def get_last_processed_date(
    connection,
    table_name: str,
) -> date | None:
    """
    Return the most recent successfully processed date
    for one Gold table.
    """

    cursor = connection.cursor()

    try:
        cursor.execute(
            """
                SELECT last_processed_date
                FROM visualization_sync_state
                WHERE table_name = %s;
            """,
            (table_name,),
        )

        row = cursor.fetchone()

        return row[0] if row else None

    finally:
        cursor.close()


def update_last_processed_date(
    connection,
    table_name: str,
    processed_date: date,
):
    """
    Store the newest successfully processed partition date.
    """

    cursor = connection.cursor()

    try:
        cursor.execute(
            """
                INSERT INTO visualization_sync_state (
                    table_name,
                    last_processed_date,
                    updated_at
                )
                VALUES (
                    %s,
                    %s,
                    CURRENT_TIMESTAMP
                )
                ON CONFLICT (table_name)
                DO UPDATE SET
                    last_processed_date =
                        EXCLUDED.last_processed_date,
                    updated_at =
                        CURRENT_TIMESTAMP;
            """,
            (
                table_name,
                processed_date,
            ),
        )

    finally:
        cursor.close()


def synchronize_table(
    connection,
    gold_bucket: str,
    table_name: str,
    table_config: dict,
    mode: str,
) -> dict[str, Any]:
    """
    Synchronize one Gold table and update its watermark
    only after a successful UPSERT.
    """

    s3_path = (
        f"s3://{gold_bucket}/{table_name}/"
    )

    print(
        f"Searching Gold dataset: {s3_path}"
    )

    all_parquet_paths = list_parquet_paths(
        s3_path
    )

    if not all_parquet_paths:
        print(
            f"No Parquet files found for "
            f"'{table_name}'."
        )

        return {
            "dates": [],
            "rows": 0,
        }

    available_dates = sorted(
        {
            partition_date
            for path in all_parquet_paths
            if (
                partition_date :=
                extract_date_partition(path)
            ) is not None
        }
    )

    if not available_dates:
        raise ValueError(
            f"Dataset '{table_name}' does not contain "
            "date=YYYY-MM-DD partitions."
        )

    last_processed_date = get_last_processed_date(
        connection=connection,
        table_name=table_name,
    )

    if mode == "full" or last_processed_date is None:
        selected_dates = available_dates
    else:
        # The watermark date is processed again.
        # UPSERT prevents duplicates while allowing corrections.
        selected_dates = [
            partition_date
            for partition_date in available_dates
            if partition_date >= last_processed_date
        ]

    if not selected_dates:
        print(
            f"No new partitions found for "
            f"'{table_name}'."
        )

        return {
            "dates": [],
            "rows": 0,
        }

    selected_date_set = set(
        selected_dates
    )

    selected_paths = [
        path
        for path in all_parquet_paths
        if extract_date_partition(path)
        in selected_date_set
    ]

    dataframe = read_partitioned_files(
        selected_paths
    )

    if dataframe.empty:
        print(
            f"Selected partitions for '{table_name}' "
            "contain no rows."
        )

        return {
            "dates": [],
            "rows": 0,
        }

    expected_columns = list(
        table_config["columns"].keys()
    )

    validate_dataframe_columns(
        dataframe=dataframe,
        expected_columns=expected_columns,
        table_name=table_name,
    )

    dataframe = dataframe[
        expected_columns
    ].copy()

    dataframe = normalize_dataframe_values(
        dataframe=dataframe,
        schema=table_config["columns"],
    )

    create_postgresql_table(
        connection=connection,
        table_name=table_name,
        columns=table_config["columns"],
        conflict_columns=(
            table_config["conflict_columns"]
        ),
    )

    upsert_dataframe(
        connection=connection,
        table_name=table_name,
        dataframe=dataframe,
        conflict_columns=(
            table_config["conflict_columns"]
        ),
    )

    newest_processed_date = max(
        selected_dates
    )

    update_last_processed_date(
        connection=connection,
        table_name=table_name,
        processed_date=newest_processed_date,
    )

    processed_date_strings = [
        value.isoformat()
        for value in selected_dates
    ]

    print(
        f"Synchronized {len(dataframe)} rows "
        f"into '{table_name}' for dates "
        f"{processed_date_strings}."
    )

    return {
        "dates": processed_date_strings,
        "rows": len(dataframe),
    }


def list_parquet_paths(
    s3_path: str,
) -> list[str]:
    """
    Return only Parquet objects from one Gold dataset.
    """

    return [
        path
        for path in wr.s3.list_objects(
            s3_path
        )
        if path.lower().endswith(
            ".parquet"
        )
    ]


def extract_date_partition(
    path: str,
) -> date | None:
    """
    Extract date=YYYY-MM-DD from an S3 object path.
    """

    match = DATE_PARTITION_PATTERN.search(
        path
    )

    if match is None:
        return None

    return date.fromisoformat(
        match.group(1)
    )


def extract_partitions(
    path: str,
) -> dict[str, str]:
    """
    Extract all Hive partition values from an S3 path.

    Example:
    platform=HackerNews/date=2026-07-01/file.parquet
    """

    partitions: dict[str, str] = {}

    for path_part in path.split("/"):
        if "=" not in path_part:
            continue

        key, value = path_part.split("=", 1)

        if key and value:
            partitions[key] = unquote(value)

    return partitions


def read_partitioned_files(
    paths: list[str],
) -> pd.DataFrame:
    """
    Read selected Parquet files and restore columns
    which only exist in their Hive partition paths.
    """

    frames: list[pd.DataFrame] = []

    for path in paths:
        frame = wr.s3.read_parquet(
            path=path
        )

        partitions = extract_partitions(
            path
        )

        for column_name, value in partitions.items():
            if column_name not in frame.columns:
                frame[column_name] = value

        frames.append(frame)

    if not frames:
        return pd.DataFrame()

    return pd.concat(
        frames,
        ignore_index=True,
    )


def create_postgresql_table(
    connection,
    table_name: str,
    columns: dict,
    conflict_columns: list[str],
):
    """
    Create a PostgreSQL table and its unique
    business-key constraint.
    """

    column_definitions = [
        f'"{column_name}" {column_type}'
        for column_name, column_type
        in columns.items()
    ]

    quoted_conflict_columns = ", ".join(
        f'"{column_name}"'
        for column_name in conflict_columns
    )

    sql = f"""
        CREATE TABLE IF NOT EXISTS "{table_name}" (
            {", ".join(column_definitions)},
            CONSTRAINT "uq_{table_name}"
                UNIQUE ({quoted_conflict_columns})
        );
    """

    cursor = connection.cursor()

    try:
        cursor.execute(sql)
    finally:
        cursor.close()


def upsert_dataframe(
    connection,
    table_name: str,
    dataframe: pd.DataFrame,
    conflict_columns: list[str],
):
    """
    UPSERT rows in batches to avoid creating one
    excessively large database operation.
    """

    columns = list(
        dataframe.columns
    )

    quoted_columns = ", ".join(
        f'"{column}"'
        for column in columns
    )

    placeholders = ", ".join(
        ["%s"] * len(columns)
    )

    quoted_conflict_columns = ", ".join(
        f'"{column}"'
        for column in conflict_columns
    )

    update_columns = [
        column
        for column in columns
        if column not in conflict_columns
    ]

    if update_columns:
        update_clause = ", ".join(
            f'"{column}" = EXCLUDED."{column}"'
            for column in update_columns
        )

        conflict_action = (
            f"DO UPDATE SET {update_clause}"
        )
    else:
        conflict_action = "DO NOTHING"

    sql = f"""
        INSERT INTO "{table_name}" (
            {quoted_columns}
        )
        VALUES (
            {placeholders}
        )
        ON CONFLICT (
            {quoted_conflict_columns}
        )
        {conflict_action};
    """

    rows = [
        tuple(
            convert_value(value)
            for value in row
        )
        for row in dataframe.itertuples(
            index=False,
            name=None,
        )
    ]

    cursor = connection.cursor()

    try:
        for start in range(
            0,
            len(rows),
            BATCH_SIZE,
        ):
            batch = rows[
                start:start + BATCH_SIZE
            ]

            cursor.executemany(
                sql,
                batch,
            )

    finally:
        cursor.close()


def validate_dataframe_columns(
    dataframe: pd.DataFrame,
    expected_columns: list[str],
    table_name: str,
):
    """
    Confirm that the Gold schema matches the
    PostgreSQL schema expected by visualization.
    """

    missing_columns = [
        column
        for column in expected_columns
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Gold table '{table_name}' is missing "
            f"columns {missing_columns}. "
            f"Available columns: "
            f"{list(dataframe.columns)}"
        )


def normalize_dataframe_values(
    dataframe: pd.DataFrame,
    schema: dict[str, str],
) -> pd.DataFrame:
    """
    Convert Parquet values to PostgreSQL-compatible
    Python values.
    """

    for column_name, sql_type in schema.items():
        normalized_type = sql_type.upper()

        if normalized_type.startswith("DATE"):
            dataframe[column_name] = pd.to_datetime(
                dataframe[column_name],
                errors="coerce",
            ).dt.date

        elif normalized_type.startswith(
            (
                "BIGINT",
                "INTEGER",
            )
        ):
            dataframe[column_name] = pd.to_numeric(
                dataframe[column_name],
                errors="coerce",
            ).astype("Int64")

        elif normalized_type.startswith(
            (
                "DOUBLE",
                "REAL",
                "NUMERIC",
                "DECIMAL",
            )
        ):
            dataframe[column_name] = pd.to_numeric(
                dataframe[column_name],
                errors="coerce",
            )

        elif normalized_type.startswith("TEXT"):
            dataframe[column_name] = (
                dataframe[column_name].map(
                    lambda value: (
                        None
                        if pd.isna(value)
                        else str(value)
                    )
                )
            )

    return dataframe.astype(object).where(
        pd.notna(dataframe),
        None,
    )


def convert_value(
    value: Any,
):
    """
    Convert pandas and NumPy scalar values to ordinary
    Python values supported by pg8000.
    """

    if value is None:
        return None

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    if isinstance(
        value,
        (
            datetime,
            date,
        ),
    ):
        return value

    if isinstance(value, Decimal):
        return float(value)

    if hasattr(value, "item"):
        return value.item()

    return value


def require_environment_variable(
    variable_name: str,
) -> str:
    """
    Read a required environment variable or fail
    with a clear error.
    """

    value = os.environ.get(
        variable_name
    )

    if value is None or value.strip() == "":
        raise ValueError(
            f"Required environment variable "
            f"'{variable_name}' is not configured."
        )

    return value