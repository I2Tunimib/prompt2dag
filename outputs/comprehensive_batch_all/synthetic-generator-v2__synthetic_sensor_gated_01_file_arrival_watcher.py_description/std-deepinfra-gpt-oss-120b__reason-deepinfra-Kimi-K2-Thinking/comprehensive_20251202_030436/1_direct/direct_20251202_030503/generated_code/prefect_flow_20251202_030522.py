import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@task(retries=2, retry_delay_seconds=300)
def wait_for_file(
    directory: Path,
    pattern: str = "transactions_{date}.csv",
    poll_interval: int = 30,
    timeout_hours: int = 24,
) -> Path:
    """
    Poll a directory for a file matching the given pattern.

    Args:
        directory: Directory to monitor.
        pattern: Filename pattern with a ``{date}`` placeholder.
        poll_interval: Seconds between polls.
        timeout_hours: Maximum hours to wait before raising an error.

    Returns:
        Path to the discovered file.

    Raises:
        FileNotFoundError: If the file is not found within the timeout.
    """
    deadline = datetime.utcnow() + timedelta(hours=timeout_hours)
    target_date = datetime.utcnow().strftime("%Y%m%d")
    expected_name = pattern.format(date=target_date)
    target_path = directory / expected_name

    logger.info("Waiting for file %s in %s", expected_name, directory)

    while datetime.utcnow() < deadline:
        if target_path.is_file():
            logger.info("File found: %s", target_path)
            return target_path
        logger.debug("File not found, sleeping %s seconds", poll_interval)
        time.sleep(poll_interval)

    raise FileNotFoundError(f"File {expected_name} not found within {timeout_hours} hours.")


@task(retries=2, retry_delay_seconds=300)
def validate_schema(file_path: Path) -> pd.DataFrame:
    """
    Validate the schema of a CSV file.

    Expected columns:
        - transaction_id (string)
        - customer_id (string)
        - amount (decimal/float)
        - transaction_date (date)

    Args:
        file_path: Path to the CSV file.

    Returns:
        pandas.DataFrame with validated and typed data.

    Raises:
        ValueError: If validation fails.
    """
    logger.info("Reading CSV file %s", file_path)
    df = pd.read_csv(file_path)

    expected_columns = ["transaction_id", "customer_id", "amount", "transaction_date"]
    missing = set(expected_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    # Enforce column order (optional)
    df = df[expected_columns]

    # Validate data types
    if not pd.api.types.is_string_dtype(df["transaction_id"]):
        raise ValueError("transaction_id must be a string.")
    if not pd.api.types.is_string_dtype(df["customer_id"]):
        raise ValueError("customer_id must be a string.")
    if not pd.api.types.is_numeric_dtype(df["amount"]):
        raise ValueError("amount must be numeric.")
    try:
        df["transaction_date"] = pd.to_datetime(df["transaction_date"]).dt.date
    except Exception as exc:
        raise ValueError("transaction_date must be a valid date.") from exc

    logger.info("Schema validation passed for %s", file_path)
    return df


@task(retries=2, retry_delay_seconds=300)
def load_db(df: pd.DataFrame, table_name: str = "transactions") -> None:
    """
    Load a DataFrame into a PostgreSQL table.

    Args:
        df: Validated DataFrame.
        table_name: Destination table name (default: ``transactions``).

    Raises:
        Exception: If the load operation fails.
    """
    # Connection details – adjust as needed or source from environment variables.
    user = "postgres"
    password = ""  # Empty password for local dev; replace with secure handling.
    host = "localhost"
    port = 5432
    database = "postgres"

    connection_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_url)

    logger.info("Loading %d records into %s.public.%s", len(df), database, table_name)
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            schema="public",
            if_exists="append",
            index=False,
            method="multi",
        )
        logger.info("Data load completed successfully.")
    finally:
        engine.dispose()


@flow
def daily_transaction_pipeline() -> None:
    """
    Orchestrates the daily transaction ingestion pipeline.

    Steps:
        1. Wait for the daily CSV file.
        2. Validate its schema.
        3. Load the validated data into PostgreSQL.
    """
    incoming_dir = Path("/data/incoming")
    file_path = wait_for_file(directory=incoming_dir)
    validated_df = validate_schema(file_path=file_path)
    load_db(df=validated_df)


if __name__ == "__main__":
    # Note: In production, configure a Prefect deployment with a daily schedule
    # starting 2024-01-01, catchup disabled, and the retry policy defined above.
    daily_transaction_pipeline()