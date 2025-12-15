from dagster import op, job, resource, RetryPolicy, In, Out, execute_in_process
from datetime import datetime, timedelta
from typing import List, Dict

# Simplified resource for database connection
@resource
def database_conn():
    return {
        'host': 'localhost',
        'port': 5432,
        'user': 'user',
        'password': 'password',
        'dbname': 'database'
    }

# Op to wait for the partition to become available
@op(required_resource_keys={'database_conn'}, retry_policy=RetryPolicy(max_retries=2, delay=300))
def wait_partition(context):
    """
    Polls the database information schema to detect when the daily partition for the orders table becomes available.
    """
    conn_info = context.resources.database_conn
    # Simplified SQL query to check partition availability
    query = f"SELECT * FROM information_schema.tables WHERE table_name = 'orders_{datetime.now().strftime('%Y%m%d')}'"
    # Simulate database check
    partition_available = True  # Replace with actual database check
    if not partition_available:
        raise Exception("Partition not available yet")
    return partition_available

# Op to extract new orders from the partitioned database table
@op(required_resource_keys={'database_conn'})
def extract_incremental(context, partition_available: bool):
    """
    Executes a SQL query to extract new orders data from the orders table filtered by the current date partition.
    """
    conn_info = context.resources.database_conn
    # Simplified SQL query to extract new orders
    query = f"SELECT * FROM orders_{datetime.now().strftime('%Y%m%d')}"
    # Simulate data extraction
    extracted_data = [{'order_id': 1, 'customer_id': 101, 'amount': 100.0, 'quantity': 2, 'timestamp': '2024-01-01T12:00:00Z'}]
    return extracted_data

# Op to transform the extracted orders data
@op
def transform(context, extracted_data: List[Dict]):
    """
    Processes the extracted orders data by cleaning customer information, validating order amounts and quantities, and formatting timestamps to ISO standard.
    """
    transformed_data = []
    for record in extracted_data:
        # Simplified data transformation
        record['timestamp'] = datetime.fromisoformat(record['timestamp']).isoformat()
        transformed_data.append(record)
    return transformed_data

# Op to load the transformed records into the data warehouse
@op(required_resource_keys={'database_conn'})
def load(context, transformed_data: List[Dict]):
    """
    Upserts the transformed records into a fact_orders table in the data warehouse and updates related metrics and aggregates.
    """
    conn_info = context.resources.database_conn
    # Simulate data loading
    for record in transformed_data:
        # Simplified SQL upsert query
        query = f"INSERT INTO fact_orders (order_id, customer_id, amount, quantity, timestamp) VALUES ({record['order_id']}, {record['customer_id']}, {record['amount']}, {record['quantity']}, '{record['timestamp']}') ON CONFLICT (order_id) DO UPDATE SET amount = EXCLUDED.amount, quantity = EXCLUDED.quantity, timestamp = EXCLUDED.timestamp"
        # Simulate database execution
        print(query)

# Define the job with the specified dependencies
@job(
    resource_defs={'database_conn': database_conn},
    description="Daily ETL pipeline that processes incremental orders data.",
    tags={"dagster/priority": "1"},
    tags={"dagster/skip_reason": "Partition not available yet"},
    tags={"dagster/retry_policy": "2 retries with 5-minute delays"},
    tags={"dagster/start_date": "2024-01-01"},
    tags={"dagster/catchup": "False"},
    tags={"dagster/schedule_interval": "@daily"}
)
def daily_orders_etl():
    partition_available = wait_partition()
    extracted_data = extract_incremental(partition_available)
    transformed_data = transform(extracted_data)
    load(transformed_data)

# Execute the job if this script is run directly
if __name__ == '__main__':
    result = daily_orders_etl.execute_in_process()