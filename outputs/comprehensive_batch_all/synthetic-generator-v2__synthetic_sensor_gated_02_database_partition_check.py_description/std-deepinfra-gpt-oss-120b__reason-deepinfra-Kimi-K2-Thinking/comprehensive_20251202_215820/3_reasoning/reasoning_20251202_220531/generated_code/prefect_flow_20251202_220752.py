from prefect import flow, task
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import time
import os


def get_db_connection(conn_id: str):
    """Get database connection - implement with your actual DB client."""
    # In production, retrieve connection string from environment:
    # conn_string = os.getenv(f"DB_CONN_{conn_id.upper()}")
    # from sqlalchemy import create_engine
    # return create_engine(conn_string)
    
    # Mock implementation for demonstration
    class MockConnection:
        def execute(self, query: str):
            print(f"Executing: {query[:100]}...")
            if "information_schema.partitions" in query:
                return [(1,)]
            return []
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
    return MockConnection()


@task(
    name="wait_for_partition",
    retries=2,
    retry_delay_seconds=300,
    timeout_seconds=3600,
)
def wait_for_partition(partition_date: str, conn_id: str = "database_conn") -> bool:
    """
    Polls the database information schema to detect when the daily partition
    for the orders table becomes available. Uses 5-minute poke interval.
    """
    print(f"Polling for partition: orders_{partition_date}")
    
    poke_interval = 300  # 5 minutes
    
    while True:
        with get_db_connection(conn_id) as conn:
            query = f"""
            SELECT 1 
            FROM information_schema.partitions 
            WHERE table_name = 'orders' 
            AND partition_name = 'orders_{partition_date}'
            LIMIT 1
            """
            result = conn.execute(query)
            if len(list(result)) > 0:
                print(f"Partition orders_{partition_date} is available")
                return True
        
        print(f"Partition not found. Waiting {poke_interval}s...")
        time.sleep(poke_interval)


@task(
    name="extract_incremental",
    retries=2,
    retry_delay_seconds=300,
)
def extract_incremental(partition_date: str, conn_id: str = "database_conn") -> List[Dict[str, Any]]:
    """
    Extracts new orders data from the orders table filtered by partition date.
    """
    print(f"Extracting orders for partition: {partition_date}")
    
    with get_db_connection(conn_id) as conn:
        query = f"""
        SELECT order_id, customer_id, order_amount, order_quantity, 
               order_timestamp, customer_info
        FROM orders