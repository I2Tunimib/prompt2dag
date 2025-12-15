from dagster import op, job, sensor, RunRequest, RunStatusSensorContext, build_resources, resource, RetryPolicy, daily_schedule
from datetime import datetime, timedelta
import time

# Resources
@resource(config_schema={"db_conn": str})
def database_resource(context):
    return context.resource_config["db_conn"]

# Ops
@op(required_resource_keys={"database"})
def wait_partition(context):
    """Polls the database to detect when the daily partition for the orders table becomes available."""
    db_conn = context.resources.database
    poke_interval = 300  # 5 minutes
    timeout = 3600  # 1 hour
    start_time = time.time()
    
    while True:
        if time.time() - start_time > timeout:
            raise Exception("Partition detection timed out.")
        
        # Simulate partition check
        partition_exists = check_partition_exists(db_conn, datetime.now().strftime("%Y-%m-%d"))
        if partition_exists:
            break
        
        time.sleep(poke_interval)

def check_partition_exists(db_conn, partition_date):
    # Simulate partition check query
    return True  # Replace with actual logic

@op
def extract_incremental(context):
    """Extracts new orders data from the orders table filtered by the current date partition."""
    db_conn = context.resources.database
    partition_date = datetime.now().strftime("%Y-%m-%d")
    
    # Simulate SQL query execution
    orders_data = fetch_orders_data(db_conn, partition_date)
    context.log.info(f"Extracted {len(orders_data)} orders for partition {partition_date}")
    return orders_data

def fetch_orders_data(db_conn, partition_date):
    # Simulate fetching orders data
    return [{"order_id": 1, "customer_id": 101, "amount": 100.0, "quantity": 2, "timestamp": "2024-01-01T12:00:00Z"}]

@op
def transform(context, orders_data):
    """Processes the extracted orders data by cleaning customer information, validating order amounts and quantities, and formatting timestamps to ISO standard."""
    transformed_data = []
    for order in orders_data:
        # Simulate data transformation
        transformed_order = {
            "order_id": order["order_id"],
            "customer_id": order["customer_id"],
            "amount": round(order["amount"], 2),
            "quantity": int(order["quantity"]),
            "timestamp": datetime.fromisoformat(order["timestamp"]).isoformat()
        }
        transformed_data.append(transformed_order)
    context.log.info(f"Transformed {len(transformed_data)} orders")
    return transformed_data

@op
def load(context, transformed_data):
    """Upserts the transformed records into a fact_orders table in the data warehouse and updates related metrics and aggregates."""
    db_conn = context.resources.database
    
    # Simulate data loading
    for order in transformed_data:
        upsert_order(db_conn, order)
    context.log.info(f"Loaded {len(transformed_data)} orders into the data warehouse")

def upsert_order(db_conn, order):
    # Simulate upsert operation
    pass

# Job
@job(
    resource_defs={"database": database_resource},
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def daily_etl_pipeline():
    orders_data = extract_incremental(wait_partition())
    transformed_data = transform(orders_data)
    load(transformed_data)

# Schedule
@daily_schedule(
    pipeline_name="daily_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    execution_time=time(0, 0),  # Midnight
    execution_timezone="UTC",
    catchup=False
)
def daily_etl_schedule(date):
    return RunRequest(run_key=None, run_config={"resources": {"database": {"config": {"db_conn": "your_db_conn"}}}})

# Sensor
@sensor(job=daily_etl_pipeline)
def partition_sensor(context):
    """Sensor to trigger the job when the partition is available."""
    db_conn = context.resources.database
    partition_date = datetime.now().strftime("%Y-%m-%d")
    
    if check_partition_exists(db_conn, partition_date):
        yield RunRequest(run_key=f"partition_{partition_date}", run_config={"resources": {"database": {"config": {"db_conn": "your_db_conn"}}}})

# Launch pattern
if __name__ == '__main__':
    with build_resources({"database": database_resource}) as resources:
        result = daily_etl_pipeline.execute_in_process(resources=resources)