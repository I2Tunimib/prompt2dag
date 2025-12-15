from datetime import datetime, timedelta
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import IntervalSchedule
import os
import pandas as pd
from sqlalchemy import create_engine


@task(retries=2, retry_delay_seconds=300, cache_key_fn=task_input_hash)
def wait_for_file():
    logger = get_run_logger()
    file_pattern = "transactions_*.csv"
    directory = "/data/incoming"
    timeout = 24 * 60 * 60  # 24 hours in seconds
    interval = 30  # 30 seconds
    start_time = datetime.now()

    while True:
        files = [f for f in os.listdir(directory) if f.startswith("transactions_") and f.endswith(".csv")]
        if files:
            logger.info(f"File found: {files[0]}")
            return os.path.join(directory, files[0])
        if (datetime.now() - start_time).total_seconds() > timeout:
            raise TimeoutError("File not found within the timeout period.")
        logger.info("No file found, waiting...")
        time.sleep(interval)


@task(retries=2, retry_delay_seconds=300)
def validate_schema(file_path):
    logger = get_run_logger()
    required_columns = ["transaction_id", "customer_id", "amount", "transaction_date"]
    required_dtypes = {"transaction_id": str, "customer_id": str, "amount": float, "transaction_date": "datetime64[ns]"}

    df = pd.read_csv(file_path)
    logger.info(f"Validating schema for file: {file_path}")

    # Check column names
    if not all(column in df.columns for column in required_columns):
        raise ValueError("Missing required columns in the file.")

    # Check data types
    for column, dtype in required_dtypes.items():
        if df[column].dtype != dtype:
            raise ValueError(f"Column {column} has incorrect data type.")

    logger.info("Schema validation successful.")
    return df


@task(retries=2, retry_delay_seconds=300)
def load_db(df):
    logger = get_run_logger()
    db_url = "postgresql://localhost:5432"
    engine = create_engine(db_url)
    table_name = "public.transactions"

    logger.info(f"Loading data into PostgreSQL table: {table_name}")
    df.to_sql(table_name, engine, if_exists="append", index=False)
    logger.info("Data loaded successfully.")


@flow(name="Daily Transaction Pipeline")
def daily_transaction_pipeline():
    file_path = wait_for_file()
    validated_df = validate_schema(file_path)
    load_db(validated_df)


if __name__ == "__main__":
    # Schedule: Daily execution via @daily interval
    # Start Date: January 1, 2024
    # Catchup: Disabled to prevent backfilling
    # Retry Policy: 2 retries with 5-minute delays between attempts
    schedule = IntervalSchedule(interval=timedelta(days=1), start_date=datetime(2024, 1, 1))
    deployment = Deployment.build_from_flow(
        flow=daily_transaction_pipeline,
        name="Daily Transaction Pipeline",
        schedule=schedule,
        catchup=False,
    )
    deployment.apply()

    daily_transaction_pipeline()