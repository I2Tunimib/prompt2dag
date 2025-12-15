from datetime import datetime, timedelta
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.filesystems import LocalFileSystem
from prefect.blocks.system import Secret
import pandas as pd
import psycopg2
import os
import time

@task(retries=2, retry_delay_seconds=300, cache_key_fn=task_input_hash)
def wait_for_file():
    logger = get_run_logger()
    directory = "/data/incoming"
    pattern = "transactions_*.csv"
    timeout = 24 * 60 * 60  # 24 hours
    interval = 30  # 30 seconds
    start_time = time.time()

    while True:
        files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f)) and f.startswith("transactions_")]
        if files:
            logger.info(f"File found: {files[0]}")
            return os.path.join(directory, files[0])
        if time.time() - start_time > timeout:
            logger.error("Timeout: No file found within 24 hours.")
            raise TimeoutError("No file found within 24 hours.")
        time.sleep(interval)

@task(retries=2, retry_delay_seconds=300)
def validate_schema(file_path):
    logger = get_run_logger()
    required_columns = ["transaction_id", "customer_id", "amount", "transaction_date"]
    required_dtypes = {"transaction_id": str, "customer_id": str, "amount": float, "transaction_date": "datetime64[ns]"}

    df = pd.read_csv(file_path)
    missing_columns = set(required_columns) - set(df.columns)
    if missing_columns:
        logger.error(f"Missing columns: {missing_columns}")
        raise ValueError(f"Missing columns: {missing_columns}")

    for col, dtype in required_dtypes.items():
        if df[col].dtype != dtype:
            logger.error(f"Column {col} has incorrect data type: {df[col].dtype}, expected {dtype}")
            raise ValueError(f"Column {col} has incorrect data type: {df[col].dtype}, expected {dtype}")

    logger.info("Schema validation successful.")
    return df

@task(retries=2, retry_delay_seconds=300)
def load_db(df, file_path):
    logger = get_run_logger()
    db_params = {
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": Secret.load("postgres-password").get(),
        "dbname": "postgres"
    }

    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    table_name = "public.transactions"
    with open(file_path, 'r') as f:
        next(f)  # Skip the header row
        cursor.copy_from(f, table_name, sep=',')

    conn.commit()
    cursor.close()
    conn.close()
    logger.info("Data loaded to PostgreSQL successfully.")

@flow(name="Daily Transaction Pipeline", retries=2, retry_delay_seconds=300)
def daily_transaction_pipeline():
    file_path = wait_for_file()
    df = validate_schema(file_path)
    load_db(df, file_path)

if __name__ == "__main__":
    daily_transaction_pipeline()

# Deployment/Schedule Configuration (optional)
# Schedule: Daily execution via @daily interval
# Start Date: January 1, 2024
# Catchup: Disabled to prevent backfilling
# Retry Policy: 2 retries with 5-minute delays between attempts