from datetime import datetime, timedelta
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import IntervalSchedule
import os
import pandas as pd
import psycopg2
from psycopg2 import sql


@task(retries=2, retry_delay_seconds=300, cache_key_fn=task_input_hash)
def wait_for_file():
    logger = get_run_logger()
    file_pattern = "transactions_*.csv"
    directory = "/data/incoming"
    timeout = 24 * 60 * 60  # 24 hours
    interval = 30  # 30 seconds
    start_time = datetime.now()

    while True:
        files = [f for f in os.listdir(directory) if f.startswith("transactions_") and f.endswith(".csv")]
        if files:
            logger.info(f"File found: {files[0]}")
            return os.path.join(directory, files[0])
        if (datetime.now() - start_time).total_seconds() > timeout:
            raise TimeoutError("File not found within the timeout period.")
        logger.info("File not found, waiting...")
        time.sleep(interval)


@task
def validate_schema(file_path):
    logger = get_run_logger()
    required_columns = ["transaction_id", "customer_id", "amount", "transaction_date"]
    required_dtypes = {"transaction_id": str, "customer_id": str, "amount": float, "transaction_date": "datetime64[ns]"}

    df = pd.read_csv(file_path)
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    for col, dtype in required_dtypes.items():
        if df[col].dtype != dtype:
            raise ValueError(f"Column {col} has incorrect data type: {df[col].dtype}, expected {dtype}")

    logger.info("Schema validation passed.")
    return df


@task
def load_db(df, file_path):
    logger = get_run_logger()
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="postgres",
        password="password",
        dbname="mydatabase"
    )
    cur = conn.cursor()

    table_name = "public.transactions"
    with open(file_path, 'r') as f:
        next(f)  # Skip the header row
        cur.copy_from(f, table_name, sep=',')

    conn.commit()
    cur.close()
    conn.close()
    logger.info("Data loaded to PostgreSQL.")


@flow(name="Daily Transaction Pipeline")
def daily_transaction_pipeline():
    file_path = wait_for_file()
    validated_df = validate_schema(file_path)
    load_db(validated_df, file_path)


if __name__ == "__main__":
    # Schedule: Daily execution via @daily interval
    # Start Date: January 1, 2024
    # Catchup: Disabled to prevent backfilling
    # Retry Policy: 2 retries with 5-minute delays between attempts
    schedule = IntervalSchedule(interval=timedelta(days=1), start_date=datetime(2024, 1, 1))
    deployment = Deployment.build_from_flow(
        flow=daily_transaction_pipeline,
        name="daily-transaction-pipeline",
        schedule=schedule,
        catchup=False
    )
    deployment.apply()

    daily_transaction_pipeline()