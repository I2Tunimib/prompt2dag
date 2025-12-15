import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash
from sqlalchemy import create_engine


@task(
    retries=2,
    retry_delay_seconds=300,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
)
def wait_for_file() -> Path:
    """
    Poll the ``/data/incoming`` directory for a CSV file matching the pattern
    ``transactions_YYYYMMDD.csv`` for the current UTC date.

    The task checks every 30 seconds and times out after 24 hours.
    Returns the absolute path to the discovered file.
    """
    directory = Path("/data/incoming")
    target_date = datetime.utcnow().date()
    filename = f"transactions_{target_date:%Y%m%d}.csv"
    file_path = directory / filename

    max_wait_seconds = 24 * 60 * 60
    poll_interval = 30
    elapsed = 0

    while elapsed < max_wait_seconds:
        if file_path.is_file():
            return file_path.resolve()
        time.sleep(poll_interval)
        elapsed += poll_interval

    raise FileNotFoundError(
        f"File {filename} not found in {directory} after 24 hours."
    )


@task(
    retries=2,
    retry_delay_seconds=300,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
)
def validate_schema(file_path: Path) -> None:
    """
    Validate that the CSV file contains the expected columns and data types.

    Expected columns:
        - transaction_id (string)
        - customer_id (string)
        - amount (decimal/float)
        - transaction_date (date)

    Raises:
        ValueError: If validation fails.
    """
    expected_columns = ["transaction_id", "customer_id", "amount", "transaction_date"]
    df = pd.read_csv(file_path)

    # Column presence check
    missing = set(expected_columns) - set(df.columns)
    extra = set(df.columns) - set(expected_columns)
    if missing:
        raise ValueError(f"Missing columns in {file_path.name}: {missing}")
    if extra:
        raise ValueError(f"Unexpected columns in {file_path.name}: {extra}")

    # Data type checks
    if not pd.api.types.is_string_dtype(df["transaction_id"]):
        raise ValueError("transaction_id must be of string type")
    if not pd.api.types.is_string_dtype(df["customer_id"]):
        raise ValueError("customer_id must be of string type")
    if not pd.api.types.is_numeric_dtype(df["amount"]):
        raise ValueError("amount must be numeric (decimal) type")
    try:
        pd.to_datetime(df["transaction_date"], format="%Y-%m-%d", errors="raise")
    except Exception as exc:
        raise ValueError("transaction_date must be a valid date") from exc


@task(
    retries=2,
    retry_delay_seconds=300,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
)
def load_db(file_path: Path) -> None:
    """
    Load the validated CSV data into the PostgreSQL table ``public.transactions``.

    Connection details are read from environment variables when available,
    otherwise default local credentials are used.
    """
    # Connection configuration
    db_user = os.getenv("POSTGRES_USER", "postgres")
    db_password = os.getenv("POSTGRES_PASSWORD", "postgres")
    db_host = os.getenv("POSTGRES_HOST", "localhost")
    db_port = os.getenv("POSTGRES_PORT", "5432")
    db_name = os.getenv("POSTGRES_DB", "postgres")

    engine_url = (
        f"postgresql+psycopg2://{db_user}:{db_password}"
        f"@{db_host}:{db_port}/{db_name}"
    )
    engine = create_engine(engine_url)

    df = pd.read_csv(file_path, parse_dates=["transaction_date"])

    # Ensure column order matches target table
    df = df[
        ["transaction_id", "customer_id", "amount", "transaction_date"]
    ]

    # Load data
    df.to_sql(
        name="transactions",
        con=engine,
        schema="public",
        if_exists="append",
        index=False,
        method="multi",
    )


@flow
def daily_transaction_pipeline() -> None:
    """
    Orchestrates the daily transaction ingestion pipeline:
    wait_for_file → validate_schema → load_db
    """
    file_path = wait_for_file()
    validate_schema(file_path)
    load_db(file_path)


if __name__ == "__main__":
    # For local execution; in production, configure a Prefect deployment
    # with a daily schedule starting 2024-01-01 and catchup disabled.
    daily_transaction_pipeline()