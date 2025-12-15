import os
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import psycopg2
from prefect import flow, task
from prefect.tasks import task_input_hash


@task(
    name="wait_for_file",
    retries=2,
    retry_delay_seconds=300,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=24)
)
def wait_for_file(
    directory: str = "/data/incoming",
    file_pattern: str = "transactions_{date}.csv",
    run_date: Optional[str] = None
) -> str:
    """
    Monitors directory for file matching the pattern. Pokes every 30 seconds
    for up to 24 hours.
    """
    date_str = run_date or datetime.now().strftime("%Y%m%d")
    target_file = file_pattern.format(date=date_str)
    target_path = Path(directory) / target_file
    
    max_wait_seconds = 86400
    poke_interval = 30
    elapsed = 0
    
    while elapsed < max_wait_seconds:
        if target_path.exists():
            return str(target_path)
        time.sleep(poke_interval)
        elapsed += poke_interval
    
    raise FileNotFoundError(
        f"File {target_path} not found after {max_wait_seconds} seconds"
    )


@task(name="validate_schema", retries=2, retry_delay_seconds=300)
def validate_schema(file_path: str) -> str:
    """
    Validates CSV schema: column names and data types.
    """
    expected_columns = {
        "transaction_id": "string",
        "customer_id": "string",
        "amount": "decimal",
        "transaction_date": "date"
    }
    
    try:
        df = pd.read_csv(file_path)
        
        actual_columns = list(df.columns)
        expected_names = list(expected_columns.keys())
        
        if actual_columns != expected_names:
            raise ValueError(
                f"Column mismatch. Expected: {expected_names}, Got: {actual_columns}"
            )
        
        # Validate amount is numeric
        try:
            df["amount"] = pd.to_numeric(df["amount"])
        except (ValueError, TypeError):
            raise ValueError("Column 'amount' contains non-numeric values")
        
        # Validate transaction_date is parseable
        try:
            df["transaction_date"] = pd.to_datetime(df["transaction_date"])
        except (ValueError, TypeError):
            raise ValueError("Column 'transaction_date' contains invalid dates")
        
        return file_path
        
    except Exception as e:
        raise ValueError(f"Schema validation failed: {str(e)}")


@task(name="load_db", retries=2, retry_delay_seconds=300)
def load_db(file_path: str, table_name: str = "public.transactions") -> None:
    """
    Loads validated CSV data to PostgreSQL.
    """
    db_config = {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "database": os.getenv("POSTGRES_DB", "transactions_db"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres")
    }
    
    df = pd.read_csv(file_path)
    df["amount"] = pd.to_numeric(df["amount"])
    df["transaction_date"] = pd.to_datetime(df["transaction_date"])
    
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        
        with conn.cursor() as cursor:
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    transaction_id VARCHAR(255),
                    customer_id VARCHAR(255),
                    amount DECIMAL(10, 2),
                    transaction_date DATE
                )
            """)
            
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            
            with open(file_path, 'r') as f:
                next(f)
                cursor.copy_expert(
                    f"COPY {table_name} FROM STDIN WITH CSV", f
                )
            
            conn.commit()
            
    except psycopg2.Error as e:
        raise RuntimeError(f"Database loading failed: {str(e)}")
    finally:
        if conn:
            conn.close()


@flow(name="sensor-gated-daily-transactions-pipeline")
def sensor_gated_pipeline(run_date: Optional[str] = None):
    """
    Sensor-gated pipeline that monitors for daily transaction files,
    validates their schema, and loads them to PostgreSQL.
    
    Schedule: Daily starting 2024-01-01, catchup disabled.
    Configure at deployment with:
    --schedule cron/0 0 * * * --anchor-date 2024-01-01
    """
    file_path = wait_for_file(run_date=run_date)
    validated_path = validate_schema(file_path)
    load_db(validated_path)


if __name__ == "__main__":
    sensor_gated_pipeline()