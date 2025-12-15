from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta, datetime
from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import IntervalSchedule


@task(retries=2, retry_delay_seconds=300, cache_key_fn=task_input_hash)
def wait_partition():
    logger = get_run_logger()
    logger.info("Waiting for the daily partition to become available...")
    # Simulate partition detection logic
    # In a real scenario, this would involve querying the database information schema
    # and checking for the existence of the required partition.
    # For demonstration, we assume the partition is always available after a short delay.
    import time
    time.sleep(300)  # Simulate a 5-minute wait
    logger.info("Partition is now available.")


@task(retries=2, retry_delay_seconds=300)
def extract_incremental():
    logger = get_run_logger()
    logger.info("Extracting new orders data from the orders table...")
    # Simulate data extraction logic
    # In a real scenario, this would involve executing a SQL query to extract new orders
    # from the orders table filtered by the current date partition.
    # For demonstration, we return a sample dataset.
    sample_data = [
        {"order_id": 1, "customer_id": 101, "order_date": "2024-01-01", "amount": 100.0},
        {"order_id": 2, "customer_id": 102, "order_date": "2024-01-01", "amount": 150.0}
    ]
    logger.info("Extracted %d records.", len(sample_data))
    return sample_data


@task(retries=2, retry_delay_seconds=300)
def transform(data):
    logger = get_run_logger()
    logger.info("Transforming extracted orders data...")
    # Simulate data transformation logic
    # In a real scenario, this would involve cleaning customer information, validating order amounts and quantities,
    # and formatting timestamps to ISO standard.
    # For demonstration, we return the same data.
    transformed_data = data
    logger.info("Transformed %d records.", len(transformed_data))
    return transformed_data


@task(retries=2, retry_delay_seconds=300)
def load(data):
    logger = get_run_logger()
    logger.info("Loading transformed orders data into the data warehouse...")
    # Simulate data loading logic
    # In a real scenario, this would involve upserting the transformed records into a fact_orders table
    # in the data warehouse and updating related metrics and aggregates.
    # For demonstration, we log the data.
    for record in data:
        logger.info("Loaded record: %s", record)


@flow(name="Daily ETL Pipeline")
def daily_etl_pipeline():
    logger = get_run_logger()
    logger.info("Starting the daily ETL pipeline...")

    partition_available = wait_partition()
    extracted_data = extract_incremental.submit(wait_for=[partition_available])
    transformed_data = transform.submit(extracted_data)
    load.submit(transformed_data)

    logger.info("Daily ETL pipeline completed.")


if __name__ == "__main__":
    # Deployment configuration (optional)
    # DeploymentSpec(
    #     name="daily-etl-pipeline",
    #     flow=daily_etl_pipeline,
    #     schedule=IntervalSchedule(interval=timedelta(days=1), start_date=datetime(2024, 1, 1)),
    #     catchup=False
    # )

    daily_etl_pipeline()