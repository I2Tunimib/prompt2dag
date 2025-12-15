from prefect import flow, task
from prefect.blocks.system import Secret
import time
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Optional


@task
def wait_partition(
    partition_date: str,
    timeout_seconds: int = 3600,
    poke_interval: int = 300
) -> str:
    """
    Polls the database information schema to detect when the daily partition
    for the orders table becomes available. Uses reschedule-like behavior
    with configurable poke interval and timeout.
    """
    # Production note: Use Prefect Blocks for secure connection management
    # connection_string = Secret.load("database-connection-string").get()
    # engine = create_engine(connection_string)
    
    # Placeholder - replace with actual database connection
    engine = create_engine("postgresql://user:pass@host:port/dbname")
    
    start_time = time.time()
    partition_name = f"orders_{partition_date.replace('-', '_')}"
    
    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            raise TimeoutError(
                f"Partition {partition_name} not available after {timeout_seconds}s"
            )
        
        try:
            with engine.connect() as conn:
                result = conn.execute(
                    text("""
                        SELECT EXISTS(
                            SELECT 1 FROM information_schema.tables 
                            WHERE table_schema = 'public' 
                            AND table_name = :partition_name
                        )
                    """),
                    {"partition_name": partition_name}
                ).scalar()
                
                if result:
                    print(f"Partition {partition_name} is now available")
                    return partition_name
        except Exception as e:
            print(f"Error checking partition: {e}")
            # Continue polling on transient errors
        
        print(f"Partition {partition_name} not yet available. Waiting {poke_interval}s...")
        time.sleep(poke_interval)


@task
def extract_incremental(partition_date: str) -> pd.DataFrame:
    """
    Extracts new orders data from the daily partition.
    """
    # Production note: Use Prefect Blocks for connection management
    engine = create_engine("postgresql://user:pass@host:port/dbname")
    
    # Build partition table name
    partition_name = f"orders_{partition_date.replace('-', '_')}"
    
    # Parameterized query to prevent SQL injection
    query = text(f"""
        SELECT order_id, customer_id, order_amount, order_quantity, 
               customer_name, customer_email, order_timestamp
        FROM {partition_name}
        WHERE partition_date = :partition_date
    """)
    
    df = pd.read_sql(
        query,
        engine,
        params={"partition_date": partition_date}
    )
    print(f"Extracted {len(df)} records from {partition_name}")
    return df


@task
def transform(orders_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans customer information, validates order amounts/quantities,
    and formats timestamps to ISO standard.
    """
    if orders_df.empty:
        print("No data to transform")
        return orders_df
    
    # Clean customer information
    orders_df['customer_name'] = orders_df['customer_name'].str.strip().str.title()
    orders_df['customer_email'] = orders_df['customer_email'].str.lower().strip()
    
    # Validate order amounts and quantities (remove invalid records)
    valid_df = orders_df[
        (orders_df['order_amount'] > 0) &
        (orders_df['order_quantity'] > 0)
    ].copy()
    
    # Format timestamps to ISO standard
    valid_df['order_timestamp'] = pd.to_datetime(
        valid_df['order_timestamp']
    ).dt.strftime('%Y-%m-%dT%H:%M:%S')
    
    dropped = len(orders_df) - len(valid_df)
    print(f"Transformed {len(valid_df)} valid records (dropped {dropped})")
    return valid_df


@task
def load(transformed_df: pd.DataFrame, partition_date: str) -> None:
    """
    Upserts transformed records into fact_orders table and refreshes aggregates.
    """
    if transformed_df.empty:
        print("No data to load")
        return
    
    # Production note: Use Prefect Blocks for data warehouse connection
    dw_engine = create_engine("postgresql://user:pass@dw_host:port/dw_dbname")
    
    with dw_engine.begin() as conn:  # Use transaction
        # Bulk upsert using temporary table for better performance
        transformed_df.to_sql(
            'temp_orders',
            conn,
            if_exists='replace',
            index=False
        )
        
        # Perform upsert from temp table to fact_orders
        conn.execute(
            text("""
                INSERT INTO fact_orders (
                    order_id, customer_id, order_amount, order_quantity, 
                    customer_name, customer_email, order_timestamp, partition_date
                )
                SELECT 
                    order_id, customer_id, order_amount, order_quantity,
                    customer_name, customer_email, order_timestamp, :partition_date
                FROM temp_orders
                ON CONFLICT (order_id) DO UPDATE SET
                    customer_id = EXCLUDED.customer_id,
                    order_amount = EXCLUDED.order_amount,
                    order_quantity = EXCLUDED.order_quantity,
                    customer_name = EXCLUDED.customer_name,
                    customer_email = EXCLUDED.customer_email,
                    order_timestamp = EXCLUDED.order_timestamp,
                    partition_date = EXCLUDED.partition_date
            """),
            {"partition_date": partition_date}
        )
        
        # Update related metrics and aggregates
        conn.execute(text("""
            REFRESH MATERIALIZED VIEW CONCURRENTLY daily_order_metrics;
            REFRESH MATERIALIZED VIEW CONCURRENTLY customer_order_aggregates;
        """))
        
        # Drop temp table
        conn.execute(text("DROP TABLE temp_orders"))
    
    print(f"Loaded {len(transformed_df)} records into fact_orders")


@flow(
    name="daily-orders-etl",
    retries=2,
    retry_delay_seconds=300,
    description="Daily ETL pipeline that processes incremental orders data"
)
def daily_orders_etl(partition_date: Optional[str] = None):
    """
    Main flow orchestrating the daily orders ETL pipeline.
    Strictly sequential: wait_partition → extract_incremental → transform → load