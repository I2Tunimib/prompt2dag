import os
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import psycopg2
from prefect import flow, task
from prefect.context import get_run_context

# Configuration constants
DEFAULT_DIRECTORY = "/data/incoming"
DEFAULT_DB_HOST = "localhost"
DEFAULT_DB_PORT = 5432
DEFAULT_DB_NAME = "postgres"
DEFAULT_DB_USER = "postgres"
DEFAULT_DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")


@task(
    name="wait_for_file",
    description="Monitor directory for transaction file arrival",
    retries=2,
    retry_delay_seconds=300,
    timeout_seconds=86400
)
def wait_for_file(directory: str = DEFAULT_DIRECTORY) -> str:
    """
    Polls the directory every 30 seconds for up to 24 hours waiting for a file
    matching the pattern transactions_YYYYMMDD.csv based on the flow run date.
    """
    try:
        context = get_run_context()
        run_date = context.flow_run.expected_start_time.date()
    except Exception:
        run_date = datetime.now().date()
    
    expected_filename = f"transactions_{run_date.strftime('%Y%m%d')}.csv"
    expected_path = Path(directory) / expected_filename
    
    if not expected_path.parent.exists():
        raise FileNotFoundError(f"Directory {directory} does not exist")
    
    start_time = time.time()
    max_wait_seconds = 86400
    poll_interval_seconds = 30
    
    while time.time() - start_time < max_wait_seconds:
        if expected_path.exists() and expected_path.is_file():
            return str(expected_path)
        time.sleep(poll_interval_seconds)
    
    raise FileNotFoundError(
        f"File {expected_filename} not found in {directory} after 24 hours"
    )


@task(
    name="validate_schema",
    description="Validate CSV file schema and data types",
    retries=2,
    retry_delay_seconds=300
)
def validate_schema(file_path: str) -> bool:
    """
    Validates that the CSV has the correct column names and data types:
    - Columns: transaction_id, customer_id, amount, transaction_date
    - Types: string, string, decimal, date
    """
    expected_columns = {
        "transaction_id": "object",
        "customer_id": "object",
        "amount": "float64",
        "transaction_date": "datetime64[ns]"
    }
    
    try:
        df = pd.read_csv(file_path)
        
        actual_columns = list(df.columns)
        expected_col_names = list(expected_columns.keys())
        
        if actual_columns != expected_col_names:
            raise ValueError(
                f"Column name mismatch. Expected: {expected_col_names}, "
                f"Got: {actual_columns}"
            )
        
        if df["transaction_id"].dtype != "object":
            raise ValueError("transaction_id must be string type")
        
        if df["customer_id"].dtype != "object":
            raise ValueError("customer_id must be string type")
        
        try:
            df["amount"] = pd.to_numeric(df["amount"])
        except Exception:
            raise ValueError("amount must be numeric/decimal type")
        
        try:
            df["transaction_date"] = pd.to_datetime(df["transaction_date"])
        except Exception:
            raise ValueError("transaction_date must be date type")
        
        return True
        
    except Exception as e:
        raise ValueError(f"Schema validation failed: {e}")


@task(
    name="load_db",
    description="Load validated data to PostgreSQL",
    retries=2,
    retry_delay_seconds=300
)
def load_db(file_path: str) -> None:
    """
    Loads the validated CSV data into the public.transactions table
    in PostgreSQL running on localhost:5432.
    """
    conn_params = {
        "host": DEFAULT_DB_HOST,
        "port": DEFAULT_DB_PORT,
        "database": DEFAULT_DB_NAME,
        "user": DEFAULT_DB_USER,
        "password": DEFAULT_DB_PASSWORD
    }
    
    try:
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        with open(file_path, 'r') as f:
            next(f)
            cursor.copy_expert(
                "COPY public.transactions FROM STDIN WITH CSV",
                f
            )
        
        conn.commit()
        
    except psycopg2.Error as e:
        raise RuntimeError(f"PostgreSQL error: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()


@flow(
    name="sensor-gated-transaction-pipeline",
    description="Daily transaction file processing pipeline with file sensor gating"
)
def transaction_pipeline(directory: str = DEFAULT_DIRECTORY):
    """
    Orchestrates the sensor-gated transaction processing pipeline.
    Linear sequence: wait_for_file -> validate_schema -> load_db
    
    Deployment configuration (add via prefect.deployments.Deployment):
    - schedule: prefect.schedules.Cron(cron="0 0 * * *", anchor_date=datetime(2024, 1, 1))
    - parameters: {"directory": "/data/incoming"}
    - catchup: False
    """
    file_path = wait_for_file(directory)
    is_valid = validate_schema(file_path)
    
    if is_valid:
        load_db(file_path)
    else:
        raise ValueError("Schema validation failed, aborting load")


if __name__ == "__main__":
    transaction_pipeline()