from datetime import datetime, date, timedelta
import time
from typing import List, Dict

from dagster import (
    op,
    job,
    RetryPolicy,
    RetryRequested,
    ConfigurableResource,
    ResourceDefinition,
    ScheduleDefinition,
    DefaultScheduleStatus,
    get_dagster_logger,
)


class DatabaseResource(ConfigurableResource):
    """Stub resource representing a source database connection."""

    def get_partition_exists(self, partition_date: date) -> bool:
        """Check if the daily partition exists.

        In a real implementation this would query the information schema.
        """
        # For demonstration, assume the partition is always available.
        return True

    def fetch_orders(self, partition_date: date) -> List[Dict]:
        """Fetch new orders for the given partition date.

        Returns a list of dictionaries representing rows.
        """
        # Dummy data for illustration.
        return [
            {
                "order_id": 1,
                "customer_name": "Alice   ",
                "order_amount": 100.0,
                "quantity": 2,
                "order_timestamp": "2024-01-01 12:34:56",
            },
            {
                "order_id": 2,
                "customer_name": "Bob",
                "order_amount": -5.0,  # Invalid amount for validation demo
                "quantity": 1,
                "order_timestamp": "2024-01-01 13:00:00",
            },
        ]


class DataWarehouseResource(ConfigurableResource):
    """Stub resource representing a target data warehouse."""

    def upsert_orders(self, records: List[Dict]) -> None:
        """Upsert transformed order records into the fact_orders table."""
        logger = get_dagster_logger()
        logger.info(f"Upserting {len(records)} records into fact_orders.")
        # In a real implementation, perform upsert logic here.
        for rec in records:
            logger.debug(f"Upserted record: {rec}")


@op(
    retry_policy=RetryPolicy(
        max_retries=12,  # 12 attempts * 5 minutes ≈ 1 hour timeout
        delay=300,  # 5 minutes in seconds
    ),
    required_resource_keys={"db"},
)
def wait_partition(context) -> date:
    """Poll the source database until the daily partition becomes available."""
    logger = context.log
    partition_date = context.op_config.get("partition_date")
    if not partition_date:
        # Default to today's date if not provided.
        partition_date = date.today()
    else:
        partition_date = datetime.strptime(partition_date, "%Y-%m-%d").date()

    logger.info(f"Waiting for partition for date {partition_date.isoformat()}.")

    if context.resources.db.get_partition_exists(partition_date):
        logger.info("Required partition is available.")
        return partition_date
    else:
        logger.warning("Partition not yet available; will retry.")
        raise RetryRequested(max_retries=12, seconds_to_wait=300)


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    required_resource_keys={"db"},
)
def extract_incremental(context, partition_date: date) -> List[Dict]:
    """Extract new orders for the given partition date."""
    logger = context.log
    logger.info(f"Extracting orders for partition date {partition_date.isoformat()}.")
    orders = context.resources.db.fetch_orders(partition_date)
    logger.info(f"Extracted {len(orders)} order records.")
    return orders


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def transform(context, orders: List[Dict]) -> List[Dict]:
    """Clean, validate, and format extracted order records."""
    logger = context.log
    transformed = []
    for order in orders:
        # Clean customer name
        customer_name = order["customer_name"].strip()

        # Validate order amount and quantity
        amount = order["order_amount"]
        quantity = order["quantity"]
        if amount <= 0 or quantity <= 0:
            logger.warning(
                f"Invalid order detected (order_id={order['order_id']}), skipping."
            )
            continue

        # Convert timestamp to ISO format
        ts = datetime.strptime(order["order_timestamp"], "%Y-%m-%d %H:%M:%S")
        iso_ts = ts.isoformat()

        transformed.append(
            {
                "order_id": order["order_id"],
                "customer_name": customer_name,
                "order_amount": amount,
                "quantity": quantity,
                "order_timestamp": iso_ts,
            }
        )
    logger.info(f"Transformed {len(transformed)} valid order records.")
    return transformed


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    required_resource_keys={"dw"},
)
def load(context, transformed_orders: List[Dict]) -> None:
    """Load transformed orders into the data warehouse."""
    logger = context.log
    if not transformed_orders:
        logger.info("No records to load.")
        return
    logger.info(f"Loading {len(transformed_orders)} records into the warehouse.")
    context.resources.dw.upsert_orders(transformed_orders)


@job(
    resource_defs={
        "db": DatabaseResource(),
        "dw": DataWarehouseResource(),
    },
    description="Daily ETL pipeline that processes incremental orders data.",
)
def daily_orders_etl():
    partition_date = wait_partition()
    raw_orders = extract_incremental(partition_date)
    cleaned_orders = transform(raw_orders)
    load(cleaned_orders)


daily_orders_schedule = ScheduleDefinition(
    job=daily_orders_etl,
    cron_schedule="0 0 * * *",  # Daily at midnight UTC
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Schedule for the daily orders ETL job.",
)


if __name__ == "__main__":
    result = daily_orders_etl.execute_in_process(
        run_config={"ops": {"wait_partition": {"config": {"partition_date": None}}}}
    )
    assert result.success