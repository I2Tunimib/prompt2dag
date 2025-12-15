from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.orion.schemas.schedules import CronSchedule
from datetime import datetime, timedelta
import time

# Task definitions
@task(retries=2, retry_delay_seconds=300, cache_key_fn=task_input_hash)
def wait_partition(partition_date: str) -> bool:
    """
    Polls the database information schema to detect when the daily partition for the orders table becomes available.
    """
    logger = get_run_logger()
    logger.info(f"Polling for partition {partition_date}...")
    # Simulate database check
    time.sleep(300)  # Simulate 5-minute poke interval
    return True  # Simulate partition availability

@task(retries=2, retry_delay_seconds=300)
def extract_incremental(partition_date: str) -> list:
    """
    Executes a SQL query to extract new orders data from the orders table filtered by the current date partition.
    """
    logger = get_run_logger()
    logger.info(f"Extracting incremental orders for partition {partition_date}...")
    # Simulate data extraction
    return [{"order_id": 1, "customer_id": 101, "amount": 100.0, "quantity": 2, "timestamp": "2024-01-01T12:00:00Z"}]

@task(retries=2, retry_delay_seconds=300)
def transform(orders: list) -> list:
    """
    Processes the extracted orders data by cleaning customer information, validating order amounts and quantities, and formatting timestamps to ISO standard.
    """
    logger = get_run_logger()
    logger.info("Transforming orders data...")
    # Simulate data transformation
    transformed_orders = []
    for order in orders:
        transformed_order = {
            "order_id": order["order_id"],
            "customer_id": order["customer_id"],
            "amount": round(order["amount"], 2),
            "quantity": int(order["quantity"]),
            "timestamp": datetime.fromisoformat(order["timestamp"]).isoformat()
        }
        transformed_orders.append(transformed_order)
    return transformed_orders

@task(retries=2, retry_delay_seconds=300)
def load(transformed_orders: list) -> None:
    """
    Upserts the transformed records into a fact_orders table in the data warehouse and updates related metrics and aggregates.
    """
    logger = get_run_logger()
    logger.info("Loading transformed orders data...")
    # Simulate data loading
    for order in transformed_orders:
        logger.info(f"Upserting order: {order}")

# Flow definition
@flow(name="Daily ETL Pipeline", schedule=CronSchedule(cron="0 0 * * *", start_date=datetime(2024, 1, 1), end_date=None, timezone="UTC"))
def daily_etl_pipeline():
    """
    Orchestrates the daily ETL pipeline for processing incremental orders data.
    """
    partition_date = datetime.now().strftime("%Y-%m-%d")
    partition_available = wait_partition(partition_date)
    if partition_available:
        orders = extract_incremental(partition_date)
        transformed_orders = transform(orders)
        load(transformed_orders)

if __name__ == "__main__":
    daily_etl_pipeline()