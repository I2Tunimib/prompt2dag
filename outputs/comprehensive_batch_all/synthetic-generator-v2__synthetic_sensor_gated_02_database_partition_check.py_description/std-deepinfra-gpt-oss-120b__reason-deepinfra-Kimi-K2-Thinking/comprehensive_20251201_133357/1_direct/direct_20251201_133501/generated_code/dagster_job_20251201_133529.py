import datetime
from typing import List, Dict

from dagster import (
    op,
    job,
    RetryRequested,
    RetryPolicy,
    resource,
    schedule,
    DefaultScheduleStatus,
    Definitions,
)


@resource
def db_resource(_):
    """A minimal stub for a database resource."""

    class DB:
        def partition_available(self, partition_date: str) -> bool:
            # Placeholder implementation: always returns True.
            # Replace with actual logic to query the information schema.
            return True

        def fetch_orders(self, partition_date: str) -> List[Dict]:
            # Placeholder implementation: returns a static list.
            # Replace with actual SQL query to fetch orders for the given partition.
            return [
                {
                    "order_id": 1,
                    "customer": " Alice ",
                    "amount": 100.0,
                    "quantity": 2,
                    "timestamp": "2024-01-01 12:00:00",
                },
                {
                    "order_id": 2,
                    "customer": "Bob",
                    "amount": -5.0,
                    "quantity": 1,
                    "timestamp": "2024-01-01 13:15:30",
                },
            ]

        def upsert_orders(self, orders: List[Dict]) -> None:
            # Placeholder implementation: simply logs the upsert.
            # Replace with actual upsert logic to the data warehouse.
            print(f"Upserting {len(orders)} orders into fact_orders table.")

    return DB()


@op(
    config_schema={"partition_date": str},
    required_resource_keys={"db"},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def wait_partition(context) -> str:
    """Polls the database until the daily partition becomes available."""
    partition_date = context.op_config["partition_date"]
    db = context.resources.db
    if not db.partition_available(partition_date):
        context.log.info(
            f"Partition {partition_date} not yet available. Retrying in 5 minutes."
        )
        raise RetryRequested(max_retries=2, seconds_to_wait=300)
    context.log.info(f"Partition {partition_date} is available.")
    return partition_date


@op(
    required_resource_keys={"db"},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def extract_incremental(context, partition_date: str) -> List[Dict]:
    """Extracts new orders for the given partition."""
    db = context.resources.db
    orders = db.fetch_orders(partition_date)
    context.log.info(f"Extracted {len(orders)} orders for partition {partition_date}.")
    return orders


@op
def transform(context, orders: List[Dict]) -> List[Dict]:
    """Cleans and validates extracted orders."""
    cleaned: List[Dict] = []
    for order in orders:
        # Clean customer name
        order["customer"] = order["customer"].strip()

        # Validate amount and quantity
        if order["amount"] <= 0 or order["quantity"] <= 0:
            continue

        # Convert timestamp to ISO format
        try:
            dt = datetime.datetime.strptime(order["timestamp"], "%Y-%m-%d %H:%M:%S")
            order["timestamp"] = dt.isoformat()
        except Exception:
            continue

        cleaned.append(order)

    context.log.info(f"Transformed {len(cleaned)} valid orders.")
    return cleaned


@op(
    required_resource_keys={"db"},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def load(context, transformed_orders: List[Dict]) -> None:
    """Loads transformed orders into the data warehouse."""
    db = context.resources.db
    db.upsert_orders(transformed_orders)
    context.log.info("Load step completed successfully.")


@job(resource_defs={"db": db_resource})
def daily_etl_job():
    """Orchestrates the daily ETL pipeline."""
    partition = wait_partition()
    raw_orders = extract_incremental(partition)
    cleaned_orders = transform(raw_orders)
    load(cleaned_orders)


@schedule(
    cron_schedule="0 0 * * *",
    job=daily_etl_job,
    default_status=DefaultScheduleStatus.RUNNING,
)
def daily_etl_schedule(context):
    """Daily schedule that injects today's partition date into the pipeline."""
    partition_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return {
        "ops": {
            "wait_partition": {"config": {"partition_date": partition_date}}
        }
    }


defs = Definitions(
    jobs=[daily_etl_job],
    schedules=[daily_etl_schedule],
    resources={"db": db_resource},
)


if __name__ == "__main__":
    # Execute the job immediately with today's partition date for testing.
    today_str = datetime.date.today().isoformat()
    result = daily_etl_job.execute_in_process(
        run_config={
            "ops": {"wait_partition": {"config": {"partition_date": today_str}}}
        }
    )
    assert result.success, "Job execution failed."