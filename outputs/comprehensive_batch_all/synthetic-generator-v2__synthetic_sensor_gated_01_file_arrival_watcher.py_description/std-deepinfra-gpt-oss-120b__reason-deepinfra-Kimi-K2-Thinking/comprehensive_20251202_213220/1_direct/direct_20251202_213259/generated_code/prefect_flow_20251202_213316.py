import os
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task

# Schedule: daily execution (e.g., via Prefect deployment with @daily interval,
# start date 2024-01-01, catchup disabled)


@task(retries=2, retry_delay_seconds=300)
def wait_for_file(directory: str = "/data/incoming") -> Path:
    """Poll the directory for today's transaction CSV file.

    The sensor checks every 30 seconds for up to 24 hours.
    """
    today_str = datetime.utcnow().strftime("%Y%m%d")
    pattern = f"transactions_{today_str}.csv"
    deadline = datetime.utcnow() + timedelta(hours=24)

    while datetime.utcnow() < deadline:
        candidate = Path(directory) / pattern
        if candidate.is_file():
            return candidate
        time.sleep(30)

    raise FileNotFoundError(f"File {pattern} not found within 24 hours.")


@task(retries=2, retry_delay_seconds=300)
def validate_schema(file_path: Path) -> Path:
    """Validate the CSV file schema and data types."""
    expected_columns = ["transaction_id", "customer_id", "amount", "transaction_date"]
    df = pd.read_csv(file_path)

    missing = set(expected_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    if not pd.api.types.is_string_dtype(df["transaction_id"]):
        raise ValueError("transaction_id must be a string")
    if not pd.api.types.is_string_dtype(df["customer_id"]):
        raise ValueError("customer_id must be a string")
    if not pd.api.types.is_numeric_dtype(df["amount"]):
        raise ValueError("amount must be numeric")
    # Ensure transaction_date can be parsed as a date
    parsed_dates = pd.to_datetime(df["transaction_date"], errors="coerce")
    if parsed_dates.isna().any():
        raise ValueError("transaction_date contains invalid dates")

    return file_path


@task(retries=2, retry_delay_seconds=300)
def load_db(file_path: Path):
    """Load the validated CSV data into PostgreSQL."""
    password = os.getenv("POSTGRES_PASSWORD", "")
    engine = create_engine(f"postgresql://postgres:{password}@localhost:5432/postgres")

    df = pd.read_csv(file_path, parse_dates=["transaction_date"])
    df.to_sql(
        name="transactions",
        con=engine,
        schema="public",
        if_exists="append",
        index=False,
    )


@flow
def daily_transaction_pipeline():
    """Orchestrates the sensor‑gated transaction loading pipeline."""
    file_path = wait_for_file()
    validated_path = validate_schema(file_path)
    load_db(validated_path)


if __name__ == "__main__":
    daily_transaction_pipeline()