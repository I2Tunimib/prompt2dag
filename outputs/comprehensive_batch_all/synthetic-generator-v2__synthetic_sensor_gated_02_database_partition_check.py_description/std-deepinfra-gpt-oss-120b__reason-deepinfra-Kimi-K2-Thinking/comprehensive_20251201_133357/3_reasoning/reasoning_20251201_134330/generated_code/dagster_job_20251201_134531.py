from dagster import (
    op,
    job,
    ScheduleDefinition,
    Definitions,
    ConfigurableResource,
    RetryPolicy,
    In,
    OpExecutionContext,
)
from datetime import datetime
from typing import List, Dict, Any
import time


class DatabaseResource(ConfigurableResource):
    """Database connection resource for executing queries."""
    
    connection_string: str = "postgresql://user:pass@localhost:5432/db"
    
    def execute_query(self, query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results as a list of dictionaries."""
        # Stub implementation - replace with actual DB logic (e.g., psycopg2, sqlalchemy)
        return []
    
    def get_partition_status(self, partition_name: str) -> bool:
        """Check if a partition exists in the database."""
        # Stub implementation - replace with actual partition existence check
        query = f"SELECT 1 FROM information_schema.tables WHERE table_name = '{partition_name}'"
        result = self.execute_query(query)
        return len(result) > 0


@op(
    required_resource_keys={"database"},
    retry_policy=RetryPolicy(max_retries=2, delay=300),  # 2 retries, 5 min delay
)
def wait_partition_op(context: OpExecutionContext) -> str:
    """
    Polls the database information schema to detect when the daily partition
    for the orders table becomes available. Uses 5-minute poke interval and 1-hour timeout.
    """
    database = context.resources.database
    partition_date = datetime.now().strftime("%Y%m%d")
    partition_name = f"orders_{partition_date}"
    
    timeout_seconds = 3600  # 1 hour timeout
    poke_interval = 300  # 5 minute poke interval
    start_time = time.time()
    
    context.log.info(f"Waiting for partition {partition_name} to become available...")
    
    while True:
        if database.get_partition_status(partition_name):
            context.log.info(f"Partition {partition_name} is now available.")
            return partition_date
        
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise TimeoutError(
                f"Partition {partition_name} not available after {timeout_seconds} seconds"
            )
        
        context.log.info(f"Partition not found. Sleeping for {poke_interval} seconds...")
        time.sleep(poke_interval)


@op(
    required_resource_keys={"database"},
    ins={"partition_date": In(str)},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def extract_incremental_op(context: OpExecutionContext, partition_date: str) -> List[Dict[str, Any]]:
    """
    Extracts new orders data from the orders table filtered by the current date partition.
    """
    database = context.resources.database
    partition_name = f"orders_{partition_date}"
    
    query = f"""
        SELECT order_id, customer_id, order_amount, quantity, order_timestamp, customer_info
        FROM {partition_name}
    """
    
    context.log.info(f"Extracting data from {partition_name}")
    data = database.execute_query(query)
    context.log.info(f"Extracted {len(data)} records")
    return data


@op(
    ins={"raw_data": In(List[Dict[str, Any]])},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def transform_op(context: OpExecutionContext, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Processes the extracted orders data by cleaning customer information,
    validating order amounts and quantities, and formatting timestamps to ISO standard.
    """
    transformed_data = []
    
    for record in raw_data:
        try:
            # Clean customer information
            customer_info = record.get("customer_info", {})
            if isinstance(customer_info, str):
                customer_info = customer_info.strip()
            
            # Validate order amounts and quantities
            order_amount = float(record.get("order_amount", 0))
            quantity = int(record.get("quantity", 0))
            
            if order_amount < 0 or quantity < 0:
                context.log.warning(
                    f"Invalid data in record {record.get('order_id')}: negative values"
                )
                continue
            
            # Format timestamp to ISO standard
            order_timestamp = record.get("order_timestamp")
            if order_timestamp and isinstance(order_timestamp, datetime):
                order_timestamp = order_timestamp.isoformat()
            
            transformed_record = {
                "order_id": record.get("order_id"),
                "customer_id": record.get("customer_id"),
                "order_amount": order_amount,
                "quantity": quantity,
                "order_timestamp": order_timestamp,
                "customer_info": customer_info,
                "processed_at": datetime.now().isoformat(),
            }
            transformed_data.append(transformed_record)
            
        except (ValueError, TypeError) as e:
            context.log.warning(
                f"Error processing record {record.get('order_id')}: {e}"
            )
            continue
    
    context.log.info(f"Transformed {len(transformed_data)} records")
    return transformed_data


@op(
    required_resource_keys={"database"},
    ins={"transformed_data": In(List[Dict[str, Any]])},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def load_op(context: OpExecutionContext, transformed_data: List[Dict[str, Any]]):
    """
    Upserts the transformed records into a fact_orders table in the data warehouse
    and updates related metrics and aggregates.
    """
    database = context.resources.database
    
    if not transformed_data:
        context.log.info("No data to load")
        return
    
    context.log.info(f"Loading {len(transformed_data)} records to fact_orders")
    
    # Upsert records into fact_orders table
    for record in transformed_data:
        # In a real implementation, this would be a proper upsert query
        context.log.debug(f"Upserting order_id: {record['order_id']}")
    
    # Update metrics and aggregates
    context.log.info("Updating related metrics and aggregates")
    
    # Example aggregate update (commented out as stub)
    # total_amount = sum(r["order_amount"] for r in transformed_data)
    # database.execute_query(
    #     "UPDATE daily_metrics SET total_orders = total_orders + %s, total_amount = total_amount + %s WHERE date = CURRENT_DATE",
    #     {"orders": len(transformed_data), "amount": total_amount}
    # )
    
    context.log.info("Load operation completed successfully")


@job(
    resource_defs={"database": DatabaseResource.configure_at_launch()},
    description="Daily ETL pipeline for processing incremental orders data",
)
def daily_orders_etl_job():
    """
    Daily ETL pipeline that processes incremental orders data.
    Linear progression: wait_partition → extract_incremental → transform → load
    """
    partition_date = wait_partition_op()
    raw_data = extract_incremental_op(partition_date)
    transformed_data = transform_op(raw_data)
    load_op(transformed_data)


# Schedule: Daily execution starting 2024-01-01, catchup disabled
# Note: Dagster schedules don't have a start_date parameter like Airflow.
# The schedule is active from deployment time. Catchup is disabled by default.
daily_schedule = ScheduleDefinition(
    job=daily_orders_etl_job,
    cron_schedule="0 0 * * *",  # Daily at midnight UTC
    execution_timezone="UTC",
)


defs = Definitions(
    jobs=[daily_orders_etl_job],
    schedules=[daily_schedule],
)


if __name__ == "__main__":
    # Minimal launch pattern for local testing
    result = daily_orders_etl_job.execute_in_process(
        run_config={
            "resources": {
                "database": {
                    "config": {
                        "connection_string": "postgresql://user:pass@localhost:5432/db"
                    }
                }
            }
        }
    )