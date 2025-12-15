import os
import time
from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import create_engine, text
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect.utilities.annotations import MaxRetries, RetryDelay


def _get_engine(conn_env: str):
    """Create a SQLAlchemy engine from an environment variable."""
    conn_str = os.getenv(conn_env)
    if not conn_str:
        raise RuntimeError(f"Environment variable '{conn_env}' is not set.")
    return create_engine(conn_str)


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Poll the source database until the daily partition is available.",
)
def wait_partition(partition_date: datetime) -> bool:
    """
    Poll the source database information schema for the existence of a partition
    corresponding to ``partition_date``. Returns ``True`` when the partition is
    detected or raises after the timeout.

    Args:
        partition_date: The date for which the partition should exist.

    Returns:
        True when the partition is found.
    """
    engine = _get_engine("DATABASE_URL")
    partition_name = partition_date.strftime("%Y-%m-%d")
    query = text(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = 'orders'
          AND partition_name = :partition_name
        """
    )
    timeout = timedelta(hours=1)
    start_time = datetime.utcnow()
    poke_interval = timedelta(minutes=5)

    while datetime.utcnow() - start_time < timeout:
        with engine.connect() as conn:
            result = conn.execute(query, {"partition_name": partition_name}).fetchone()
            if result:
                return True
        time.sleep(poke_interval.total_seconds())

    raise TimeoutError(
        f"Partition for {partition_name} not found within {timeout}."
    )


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Extract incremental orders for the given partition date.",
)
def extract_incremental(partition_date: datetime) -> pd.DataFrame:
    """
    Execute a SQL query to extract new orders for ``partition_date`` from the
    source ``orders`` table.

    Args:
        partition_date: The date partition to extract.

    Returns:
        DataFrame containing the extracted rows.
    """
    engine = _get_engine("DATABASE_URL")
    sql = text(
        """
        SELECT *
        FROM orders
        WHERE DATE(partition_column) = :partition_date
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql_query(
            sql, conn, params={"partition_date": partition_date.date()}
        )
    return df


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Transform extracted orders data.",
)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate the orders data.

    - Trim whitespace from string columns.
    - Ensure order amounts and quantities are non‑negative.
    - Convert timestamp columns to ISO‑8601 strings.

    Args:
        df: Raw orders DataFrame.

    Returns:
        Cleaned and validated DataFrame.
    """
    if df.empty:
        return df

    # Example cleaning steps
    str_cols = df.select_dtypes(include=["object"]).columns
    for col in str_cols:
        df[col] = df[col].str.strip()

    numeric_cols = ["order_amount", "order_quantity"]
    for col in numeric_cols:
        if col in df.columns:
            df = df[df[col] >= 0]

    timestamp_cols = ["order_timestamp"]
    for col in timestamp_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    return df


@task(
    retries=2,
    retry_delay_seconds=300,
    description="Load transformed orders into the data warehouse.",
)
def load(df: pd.DataFrame):
    """
    Upsert the transformed records into the ``fact_orders`` table of the data
    warehouse.

    Args:
        df: Transformed orders DataFrame.
    """
    if df.empty:
        return

    engine = _get_engine("WAREHOUSE_URL")
    # Simple upsert using PostgreSQL syntax; adapt as needed for the target DW.
    upsert_sql = text(
        """
        INSERT INTO fact_orders (order_id, customer_id, order_amount,
                                 order_quantity, order_timestamp)
        VALUES (:order_id, :customer_id, :order_amount,
                :order_quantity, :order_timestamp)
        ON CONFLICT (order_id) DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            order_amount = EXCLUDED.order_amount,
            order_quantity = EXCLUDED.order_quantity,
            order_timestamp = EXCLUDED.order_timestamp;
        """
    )
    records = df.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(upsert_sql, records)


@flow(description="Daily ETL pipeline for incremental orders.")
def daily_orders_etl():
    """
    Orchestrates the ETL pipeline:
    1. Wait for the daily partition to become available.
    2. Extract new orders for the partition.
    3. Transform the extracted data.
    4. Load the transformed data into the warehouse.
    """
    partition_date = datetime.utcnow().date() - timedelta(days=1)  # previous day
    # Sensor step
    wait_partition(partition_date=datetime.combine(partition_date, datetime.min.time()))
    # Sequential ETL steps
    raw_df = extract_incremental(partition_date=datetime.combine(partition_date, datetime.min.time()))
    clean_df = transform(raw_df)
    load(clean_df)


if __name__ == "__main__":
    # Note: In production, configure a Prefect deployment with a daily schedule
    # starting on 2024-01-01 and catchup disabled.
    daily_orders_etl()