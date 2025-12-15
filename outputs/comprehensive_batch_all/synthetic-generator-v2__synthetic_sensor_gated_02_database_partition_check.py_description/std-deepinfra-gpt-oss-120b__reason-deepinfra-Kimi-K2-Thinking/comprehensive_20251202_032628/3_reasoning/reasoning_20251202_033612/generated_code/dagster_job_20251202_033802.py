from dagster import (
    op,
    job,
    ScheduleDefinition,
    Definitions,
    RetryPolicy,
    OpExecutionContext,
    ResourceDefinition,
    config_from_files,
)
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List
import pandas as pd


# Minimal database resource stub
class DatabaseResource:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
    
    def get_connection(self):
        # In real implementation, return actual DB connection
        # For this stub, we'll simulate
        return self
    
    def execute_query(self, query: str, params: Dict[str, Any] = None):
        # Stub implementation - replace with actual DB logic
        print(f"Executing query: {query}")
        if "information_schema" in query:
            # Simulate partition detection
            return [{"partition_exists": True}]
        elif "FROM orders" in query:
            # Simulate data extraction
            return [
                {
                    "order_id": 1,
                    "customer_name": "John Doe",
                    "order_amount": 100.0,
                    "quantity": 2,
                    "order_timestamp": "2024-01-01 10:00:00",
                }
            ]
        return []
    
    def execute_bulk_operation(self, query: str, data: List[Dict[str, Any]]):
        # Stub implementation
        print(f"Executing bulk operation: {query} with {len(data)} records")
        return len(data)


# Resource definition
database_resource = ResourceDefinition.hardcoded_resource(
    DatabaseResource(connection_string="postgresql://user:pass@host:5432/db"),
    description="Database connection resource",
)


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),  # 2 retries, 5 min delay
    description="Polls database information schema to detect daily partition availability",
)
def wait_partition(context: OpExecutionContext) -> bool:
    """
    Waits for the daily partition of the orders table to become available.
    Uses polling with 5-minute intervals and 1-hour timeout.
    """
    # Get database resource
    db = context.resources.database
    conn = db.get_connection()
    
    # Calculate current date partition
    current_date = datetime.now().strftime("%Y-%m-%d")
    partition_name = f"orders_{current_date.replace('-', '_')}"
    
    # Polling configuration
    poke_interval = 300  # 5 minutes in seconds
    timeout = 3600  # 1 hour in seconds
    start_time = time.time()
    
    context.log.info(f"Waiting for partition: {partition_name}")
    
    while True:
        # Check if timeout exceeded
        elapsed = time.time() - start_time
        if elapsed > timeout:
            raise TimeoutError(
                f"Timeout waiting for partition {partition_name} after {timeout} seconds"
            )
        
        # Query information schema for partition
        query = """
            SELECT EXISTS(
                SELECT 1 FROM information_schema.tables 
                WHERE table_name = %s
            ) as partition_exists
        """
        result = conn.execute_query(query, {"partition_name": partition_name})
        
        if result and result[0].get("partition_exists"):
            context.log.info(f"Partition {partition_name} is now available")
            return True
        
        # Wait before next poke
        context.log.info(
            f"Partition not found. Waiting {poke_interval} seconds before next check..."
        )
        time.sleep(poke_interval)


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Extracts new orders data from the partitioned orders table",
)
def extract_incremental(context: OpExecutionContext, partition_ready: bool) -> List[Dict[str, Any]]:
    """
    Executes SQL query to extract new orders data filtered by current date partition.
    """
    # Get database resource
    db = context.resources.database
    conn = db.get_connection()
    
    # Calculate current date partition
    current_date = datetime.now().strftime("%Y-%m-%d")
    partition_name = f"orders_{current_date.replace('-', '_')}"
    
    context.log.info(f"Extracting data from partition: {partition_name}")
    
    # Extract data from partition
    query = f"""
        SELECT 
            order_id,
            customer_name,
            order_amount,
            quantity,
            order_timestamp
        FROM {partition_name}
    """
    
    data = conn.execute_query(query)
    context.log.info(f"Extracted {len(data)} records")
    
    return data


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Cleans and validates orders data",
)
def transform(context: OpExecutionContext, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Processes extracted orders data by cleaning customer information,
    validating order amounts and quantities, and formatting timestamps to ISO standard.
    """
    if not raw_data:
        context.log.info("No data to transform")
        return []
    
    transformed_data = []
    
    for record in raw_data:
        # Clean customer name (strip whitespace, title case)
        customer_name = record.get("customer_name", "").strip().title()
        
        # Validate order amount and quantity
        order_amount = float(record.get("order_amount", 0))
        quantity = int(record.get("quantity", 0))
        
        if order_amount <= 0 or quantity <= 0:
            context.log.warning(
                f"Invalid record skipped: order_id={record.get('order_id')}, "
                f"amount={order_amount}, quantity={quantity}"
            )
            continue
        
        # Format timestamp to ISO standard
        timestamp_str = record.get("order_timestamp", "")
        try:
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
            iso_timestamp = timestamp.isoformat()
        except (ValueError, TypeError):
            context.log.warning(
                f"Invalid timestamp for order_id={record.get('order_id')}"
            )
            continue
        
        transformed_record = {
            "order_id": record["order_id"],
            "customer_name": customer_name,
            "order_amount": order_amount,
            "quantity": quantity,
            "order_timestamp": iso_timestamp,
            "processed_date": datetime.now().isoformat(),
        }
        transformed_data.append(transformed_record)
    
    context.log.info(f"Transformed {len(transformed_data)} valid records")
    return transformed_data


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Loads transformed records into fact_orders table",
)
def load(context: OpExecutionContext, transformed_data: List[Dict[str, Any]]) -> int:
    """
    Upserts transformed records into fact_orders table in data warehouse
    and updates related metrics and aggregates.
    """
    if not transformed_data:
        context.log.info("No data to load")
        return 0
    
    # Get database resource
    db = context.resources.database
    conn = db.get_connection()
    
    context.log.info(f"Loading {len(transformed_data)} records to fact_orders")
    
    # Upsert into fact_orders table
    upsert_query = """
        INSERT INTO fact_orders 
        (order_id, customer_name, order_amount, quantity, order_timestamp, processed_date)
        VALUES (%(order_id)s, %(customer_name)s, %(order_amount)s, %(quantity)s, 
                %(order_timestamp)s, %(processed_date)s)
        ON CONFLICT (order_id) DO UPDATE SET
            customer_name = EXCLUDED.customer_name,
            order_amount = EXCLUDED.order_amount,
            quantity = EXCLUDED.quantity,
            order_timestamp = EXCLUDED.order_timestamp,
            processed_date = EXCLUDED.processed_date
    """
    
    records_loaded = conn.execute_bulk_operation(upsert_query, transformed_data)
    
    # Update metrics and aggregates (simplified)
    context.log.info("Updating metrics and aggregates")
    metrics_query = """
        REFRESH MATERIALIZED VIEW CONCURRENTLY order_daily_metrics
    """
    try:
        conn.execute_query(metrics_query)
    except Exception as e:
        context.log.warning(f"Failed to refresh metrics: {e}")
    
    context.log.info(f"Successfully loaded {records_loaded} records")
    return records_loaded


@job(
    description="Daily ETL pipeline for incremental orders processing",
    resource_defs={"database": database_resource},
)
def daily_orders_etl():
    """
    Daily ETL pipeline that processes incremental orders data.
    Implements sensor-gated pattern with sequential ETL steps.
    """
    # Define the dependency chain
    partition_ready = wait_partition()
    raw_data = extract_incremental(partition_ready)
    transformed_data = transform(raw_data)
    load(transformed_data)


# Schedule definition
daily_schedule = ScheduleDefinition(
    job=daily_orders_etl,
    cron_schedule="0 0 * * *",  # Daily at midnight
    execution_timezone="UTC",
    default_status="RUNNING",
)


# Definitions object to tie everything together
defs = Definitions(
    jobs=[daily_orders_etl],
    schedules=[daily_schedule],
    resources={"database": database_resource},
)


# Minimal launch pattern
if __name__ == "__main__":
    # Execute the job in-process for testing
    result = daily_orders_etl.execute_in_process(
        run_config={
            "resources": {
                "database": {
                    "config": {
                        "connection_string": "postgresql://user:pass@host:5432/db"
                    }
                }
            }
        }
    )
    
    if result.success:
        print("Pipeline execution completed successfully")
    else:
        print("Pipeline execution failed")
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                print(f"Step failed: {event.step_key}")