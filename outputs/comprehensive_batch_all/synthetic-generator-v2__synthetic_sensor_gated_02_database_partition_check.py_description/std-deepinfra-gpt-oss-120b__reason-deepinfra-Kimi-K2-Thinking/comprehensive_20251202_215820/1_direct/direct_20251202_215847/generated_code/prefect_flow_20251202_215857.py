from datetime import datetime, timedelta, date
import time
from typing import Any

import pandas as pd
from prefect import flow, task, get_run_logger


@task(retries=2, retry_delay_seconds=300)
def wait_partition(partition_date: date) -> bool:
    """
    Poll the database information schema until the daily partition for the orders
    table becomes available.

    Args:
        partition_date: The date of the partition to wait for.

    Returns:
        True when the partition is detected.

    Note:
        This implementation simulates the check. Replace the placeholder logic
        with an actual query against your database.
    """
    logger = get_run_logger()
    timeout = timedelta(hours=1)
    poke_interval = timedelta(minutes=5)
    start_time = datetime.utcnow()
    logger.info("Waiting for partition %s to become available.", partition_date)

    while datetime.utcnow() - start_time < timeout:
        # Placeholder: assume partition becomes available after first check.
        # Replace with real check, e.g., SELECT 1 FROM information_schema...
        partition_available = True
        if partition_available:
            logger.info("Partition %s is now available.", partition_date)
            return True
        logger.debug(
            "Partition %s not yet available; sleeping for %s seconds.",
            partition_date,
            poke_interval.total_seconds(),
        )
        time.sleep(poke_interval.total_seconds())

    raise TimeoutError(
        f"Timed out after {timeout} waiting for partition {partition_date}."
    )


@task(retries=2, retry_delay_seconds=300)
def extract_incremental(partition_date: date) -> pd.DataFrame:
    """
    Extract new orders for the given partition date.

    Args:
        partition_date: The date of the partition to extract.

    Returns:
        DataFrame containing the extracted orders.
    """
    logger = get_run_logger()
    logger.info("Extracting incremental orders for partition %s.", partition_date)

    # Placeholder SQL query; replace with actual query execution.
    sql = f"""
        SELECT *
        FROM orders
        WHERE partition_date = '{partition_date}'
    """
    logger.debug("Executing SQL: %s", sql.strip())

    # Simulated data; replace with real query results.
    data = [
        {
            "order_id": 1,
            "customer_name": "Alice",
            "order_amount": 120.5,
            "quantity": 2,
            "order_timestamp": datetime.utcnow(),
        },
        {
            "order_id": 2,
            "customer_name": "Bob",
            "order_amount": 75.0,
            "quantity": 1,
            "order_timestamp": datetime.utcnow(),
        },
    ]
    df = pd.DataFrame(data)
    logger.info("Extracted %d rows.", len(df))
    return df


@task(retries=2, retry_delay_seconds=300)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate the extracted orders data.

    Args:
        df: Raw orders DataFrame.

    Returns:
        Cleaned and validated DataFrame.
    """
    logger = get_run_logger()
    logger.info("Transforming %d rows.", len(df))

    # Clean customer information (example: strip whitespace, title case)
    df["customer_name"] = (
        df["customer_name"].astype(str).str.strip().str.title()
    )

    # Validate order amounts and quantities (example: non‑negative)
    df = df[(df["order_amount"] >= 0) & (df["quantity"] > 0)]

    # Format timestamps to ISO 8601 strings
    df["order_timestamp"] = df["order_timestamp"].apply(
        lambda ts: ts.isoformat()
    )

    logger.info("Transformation complete; %d rows remain after validation.", len(df))
    return df


@task(retries=2, retry_delay_seconds=300)
def load(df: pd.DataFrame) -> None:
    """
    Upsert transformed records into the fact_orders table in the data warehouse.

    Args:
        df: Cleaned orders DataFrame.
    """
    logger = get_run_logger()
    logger.info("Loading %d rows into fact_orders.", len(df))

    # Placeholder for upsert logic; replace with actual database interaction.
    # Example: using SQLAlchemy or a database client to perform upserts.
    for _, row in df.iterrows():
        logger.debug("Upserting order_id %s", row["order_id"])
        # upsert_operation(row)

    logger.info("Load step completed.")


@flow(name="daily_orders_etl")
def etl_flow(run_date: datetime | None = None) -> Any:
    """
    Orchestrates the daily ETL pipeline for orders.

    Args:
        run_date: The execution date; defaults to the current UTC time.
    """
    if run_date is None:
        run_date = datetime.utcnow()
    partition_date = run_date.date()

    # Sensor‑gated entry
    wait_partition(partition_date)

    # Sequential ETL steps
    raw_df = extract_incremental(partition_date)
    clean_df = transform(raw_df)
    load(clean_df)


# Schedule configuration (to be defined in Prefect deployment):
# - Interval: daily
# - Start date: 2024-01-01
# - Catchup: disabled

if __name__ == "__main__":
    etl_flow()