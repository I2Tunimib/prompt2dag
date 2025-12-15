import os
import time
from datetime import datetime, timedelta

import pandas as pd
import sqlalchemy
from prefect import flow, task
from prefect.logging import get_logger


def _get_engine(conn_name: str) -> sqlalchemy.engine.Engine:
    """
    Retrieve a SQLAlchemy engine using a connection string from environment variables.

    The environment variable name is expected to be ``{conn_name.upper()}_URL``.
    """
    url = os.getenv(f"{conn_name.upper()}_URL")
    if not url:
        raise RuntimeError(f"Environment variable {conn_name.upper()}_URL is not set")
    return sqlalchemy.create_engine(url)


@task(retries=2, retry_delay_seconds=300)
def wait_partition(conn_name: str = "database_conn") -> None:
    """
    Poll the information schema until the daily partition for the ``orders`` table
    becomes available.

    The task checks every 5 minutes and times out after 1 hour.
    """
    logger = get_logger()
    engine = _get_engine(conn_name)

    poke_interval = 300  # 5 minutes in seconds
    timeout = 3600  # 1 hour in seconds
    start_time = datetime.utcnow()
    partition_name = datetime.utcnow().strftime("%Y%m%d")  # e.g., 20240101

    logger.info(
        "Waiting for partition %s of table orders (timeout %s seconds)",
        partition_name,
        timeout,
    )

    while True:
        query = sqlalchemy.text(
            """
            SELECT 1
            FROM information_schema.partitions
            WHERE table_name = 'orders'
              AND partition_name = :partition_name
            LIMIT 1
            """
        )
        with engine.connect() as conn:
            result = conn.execute(query, {"partition_name": partition_name}).fetchone()

        if result:
            logger.info("Partition %s is now available.", partition_name)
            return

        elapsed = (datetime.utcnow() - start_time).total_seconds()
        if elapsed > timeout:
            raise TimeoutError(
                f"Partition {partition_name} not found within {timeout} seconds."
            )

        logger.debug(
            "Partition %s not yet available; sleeping for %s seconds.", partition_name, poke_interval
        )
        time.sleep(poke_interval)


@task(retries=2, retry_delay_seconds=300)
def extract_incremental(conn_name: str = "database_conn") -> pd.DataFrame:
    """
    Extract new orders for the current date partition from the source database.
    """
    logger = get_logger()
    engine = _get_engine(conn_name)
    partition_date = datetime.utcnow().date()
    sql = sqlalchemy.text(
        """
        SELECT *
        FROM orders
        WHERE DATE(partition_column) = :partition_date
        """
    )
    logger.info("Extracting orders for partition date %s.", partition_date)
    with engine.connect() as conn:
        df = pd.read_sql_query(sql, conn, params={"partition_date": partition_date})
    logger.info("Extracted %d rows.", len(df))
    return df


@task(retries=2, retry_delay_seconds=300)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate the extracted orders data.

    - Trim whitespace from string columns.
    - Ensure order amounts and quantities are non‑negative.
    - Convert timestamp columns to ISO‑8601 strings.
    """
    logger = get_logger()
    logger.info("Starting transformation of %d rows.", len(df))

    # Example cleaning steps
    str_cols = df.select_dtypes(include=["object"]).columns
    for col in str_cols:
        df[col] = df[col].str.strip()

    # Validate numeric columns
    numeric_cols = ["order_amount", "order_quantity"]
    for col in numeric_cols:
        if col in df.columns:
            df = df[df[col] >= 0]

    # Convert timestamps
    timestamp_cols = ["order_timestamp", "created_at"]
    for col in timestamp_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.isoformat()

    logger.info("Transformation complete; %d rows remain after cleaning.", len(df))
    return df


@task(retries=2, retry_delay_seconds=300)
def load(df: pd.DataFrame, conn_name: str = "data_warehouse_conn") -> None:
    """
    Upsert transformed records into the ``fact_orders`` table in the data warehouse.
    """
    logger = get_logger()
    engine = _get_engine(conn_name)

    if df.empty:
        logger.warning("No data to load; exiting load step.")
        return

    logger.info("Loading %d rows into fact_orders.", len(df))

    # Simple upsert using PostgreSQL ON CONFLICT as an example.
    # Adjust the statement for the target warehouse dialect as needed.
    upsert_sql = """
    INSERT INTO fact_orders (order_id, customer_id, order_amount, order_quantity,
                            order_timestamp, created_at)
    VALUES (:order_id, :customer_id, :order_amount, :order_quantity,
            :order_timestamp, :created_at)
    ON CONFLICT (order_id) DO UPDATE SET
        customer_id = EXCLUDED.customer_id,
        order_amount = EXCLUDED.order_amount,
        order_quantity = EXCLUDED.order_quantity,
        order_timestamp = EXCLUDED.order_timestamp,
        created_at = EXCLUDED.created_at;
    """

    records = df.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text(upsert_sql), records)

    logger.info("Load step completed successfully.")


@flow
def daily_orders_etl():
    """
    Daily ETL flow for incremental orders data.

    Execution pattern:
        wait_partition → extract_incremental → transform → load
    """
    wait_partition()
    raw_df = extract_incremental()
    cleaned_df = transform(raw_df)
    load(cleaned_df)


if __name__ == "__main__":
    # Note: In production, schedule this flow with a daily deployment
    # (e.g., using Prefect's Deployment API with a cron schedule).
    daily_orders_etl()