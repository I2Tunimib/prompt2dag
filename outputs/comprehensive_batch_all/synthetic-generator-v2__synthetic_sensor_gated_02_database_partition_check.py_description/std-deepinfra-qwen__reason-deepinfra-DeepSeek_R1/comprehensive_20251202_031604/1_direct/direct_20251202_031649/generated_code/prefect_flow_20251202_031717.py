from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.orion.schemas.schedules import CronSchedule
from datetime import timedelta, datetime
from sqlalchemy import create_engine, text
import pandas as pd

# Database connection
DATABASE_CONN = "your_database_connection_string"

# Task to wait for the partition to become available
@task(retries=2, retry_delay_seconds=300, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def wait_partition(partition_date: str) -> bool:
    logger = get_run_logger()
    engine = create_engine(DATABASE_CONN)
    query = text(f"SELECT 1 FROM information_schema.partitions WHERE table_name = 'orders' AND partition_description = '{partition_date}'")
    with engine.connect() as connection:
        result = connection.execute(query).fetchone()
    if result:
        logger.info(f"Partition for {partition_date} is available.")
        return True
    else:
        logger.warning(f"Partition for {partition_date} is not available yet.")
        return False

# Task to extract incremental orders data
@task(retries=2, retry_delay_seconds=300)
def extract_incremental(partition_date: str) -> pd.DataFrame:
    logger = get_run_logger()
    engine = create_engine(DATABASE_CONN)
    query = text(f"SELECT * FROM orders WHERE partition_date = '{partition_date}'")
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
    logger.info(f"Extracted {len(df)} records for {partition_date}.")
    return df

# Task to transform the extracted data
@task(retries=2, retry_delay_seconds=300)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    # Clean customer information
    df['customer_name'] = df['customer_name'].str.strip()
    # Validate order amounts and quantities
    df = df[(df['order_amount'] > 0) & (df['order_quantity'] > 0)]
    # Format timestamps to ISO standard
    df['order_timestamp'] = pd.to_datetime(df['order_timestamp']).dt.strftime('%Y-%m-%dT%H:%M:%S')
    logger.info(f"Transformed {len(df)} records.")
    return df

# Task to load the transformed data into the data warehouse
@task(retries=2, retry_delay_seconds=300)
def load(df: pd.DataFrame) -> None:
    logger = get_run_logger()
    engine = create_engine(DATABASE_CONN)
    df.to_sql('fact_orders', engine, if_exists='append', index=False)
    logger.info(f"Loaded {len(df)} records into the data warehouse.")

# Flow to orchestrate the pipeline
@flow(name="Daily ETL Pipeline", schedule=CronSchedule(cron="0 0 * * *", start_date=datetime(2024, 1, 1), end_date=None, timezone="UTC"), catchup=False)
def daily_etl_pipeline():
    logger = get_run_logger()
    partition_date = datetime.now().strftime('%Y-%m-%d')
    logger.info(f"Starting ETL pipeline for partition date: {partition_date}")

    partition_available = wait_partition(partition_date)
    if partition_available:
        extracted_data = extract_incremental(partition_date)
        transformed_data = transform(extracted_data)
        load(transformed_data)
    else:
        logger.warning(f"Partition for {partition_date} is not available. Pipeline aborted.")

if __name__ == '__main__':
    daily_etl_pipeline()