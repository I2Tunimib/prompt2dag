from datetime import datetime, timedelta
import time
from typing import Any, Dict, List

from dagster import (
    DefaultScheduleStatus,
    RetryPolicy,
    Definitions,
    JobDefinition,
    OpExecutionContext,
    ResourceDefinition,
    ScheduleDefinition,
    job,
    op,
    resource,
)


@resource
def db_resource() -> Dict[str, Any]:
    """A minimal stub for a database connection resource."""
    # In a real deployment, return an actual connection object or client.
    return {"conn": None}


def _partition_available(partition_date: str) -> bool:
    """Stub that pretends to check the information schema for a partition."""
    # Replace with real query logic using the provided connection.
    # Here we simply assume the partition is always available.
    return True


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Polls the source database until the daily partition is available.",
)
def wait_partition(context: OpExecutionContext, db= db_resource) -> str:
    """Wait for the daily partition to become available.

    Returns the partition identifier (e.g., YYYY-MM-DD string).
    """
    partition_date = datetime.utcnow().date().isoformat()
    timeout = timedelta(hours=1)
    start_time = datetime.utcnow()
    poke_interval = timedelta(minutes=5)

    context.log.info(f"Waiting for partition {partition_date} to appear.")
    while datetime.utcnow() - start_time < timeout:
        if _partition_available(partition_date):
            context.log.info(f"Partition {partition_date} is now available.")
            return partition_date
        context.log.info(
            f"Partition {partition_date} not yet available; sleeping {poke_interval}."
        )
        time.sleep(poke_interval.total_seconds())

    raise RuntimeError(f"Timeout: partition {partition_date} did not become available.")


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Extracts new orders for the given partition from the source database.",
)
def extract_incremental(
    context: OpExecutionContext, partition: str, db= db_resource
) -> List[Dict[str, Any]]:
    """Execute a SQL query to fetch new orders for the supplied partition.

    Returns a list of order records as dictionaries.
    """
    context.log.info(f"Extracting orders for partition {partition}.")
    # Placeholder for real extraction logic.
    sample_data = [
        {
            "order_id": 1,
            "customer_name": "Alice",
            "order_amount": 120.5,
            "quantity": 2,
            "order_timestamp": "2024-01-01 08:15:00",
        },
        {
            "order_id": 2,
            "customer_name": "Bob",
            "order_amount": 75.0,
            "quantity": 1,
            "order_timestamp": "2024-01-01 09:30:00",
        },
    ]
    return sample_data


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Cleans, validates, and formats extracted order records.",
)
def transform(
    context: OpExecutionContext, orders: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Apply cleaning, validation, and timestamp formatting to raw orders."""
    context.log.info(f"Transforming {len(orders)} order records.")
    transformed = []
    for order in orders:
        # Simple cleaning: strip whitespace from customer name.
        customer_name = order["customer_name"].strip()
        # Validation: ensure positive amounts and quantities.
        if order["order_amount"] <= 0 or order["quantity"] <= 0:
            context.log.warning(f"Invalid order detected: {order['order_id']}")
            continue
        # Timestamp formatting to ISO 8601.
        ts = datetime.strptime(order["order_timestamp"], "%Y-%m-%d %H:%M:%S")
        iso_ts = ts.isoformat()
        transformed.append(
            {
                "order_id": order["order_id"],
                "customer_name": customer_name,
                "order_amount": order["order_amount"],
                "quantity": order["quantity"],
                "order_timestamp": iso_ts,
            }
        )
    context.log.info(f"{len(transformed)} records passed transformation.")
    return transformed


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Upserts transformed orders into the fact_orders table of the data warehouse.",
)
def load(
    context: OpExecutionContext,
    transformed_orders: List[Dict[str, Any]],
    db= db_resource,
) -> None:
    """Load transformed records into the target data warehouse."""
    context.log.info(f"Loading {len(transformed_orders)} records into fact_orders.")
    # Placeholder for real upsert logic.
    for order in transformed_orders:
        context.log.debug(f"Upserting order_id={order['order_id']}")
    context.log.info("Load step completed.")


@job(resource_defs={"db": db_resource})
def etl_job():
    """Sequential ETL job gated by partition availability."""
    partition = wait_partition()
    raw_orders = extract_incremental(partition)
    cleaned_orders = transform(raw_orders)
    load(cleaned_orders)


daily_schedule = ScheduleDefinition(
    job=etl_job,
    cron_schedule="0 0 * * *",  # Runs daily at midnight UTC.
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Daily schedule for the ETL job.",
)


defs = Definitions(
    jobs=[etl_job],
    schedules=[daily_schedule],
    resources={"db": db_resource},
)


if __name__ == "__main__":
    result = etl_job.execute_in_process()
    if result.success:
        print("ETL job completed successfully.")
    else:
        print("ETL job failed.")