import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2
from prefect import flow, task
from prefect.context import get_run_context


@task
def wait_for_file(
    base_dir: str = "/data/incoming",
    filename_pattern: str = "transactions_{date_str}.csv",
    poke_interval: int = 30,
    timeout: int = 86400,
) -> str:
    """
    Wait for a file matching the pattern to appear in the directory.
    Pokes every 30 seconds for up to 24 hours.
    """
    logger = wait_for_file.get_logger()
    
    try:
        context = get_run_context()
        run_date = context.flow_run.expected_start_time
    except Exception:
        run_date = datetime.now()
    
    date_str = run_date.strftime("%Y%m%d")
    filename = filename_pattern.format(date_str=date_str)
    file_path = Path(base_dir) / filename
    
    logger.info(f"Waiting for file: {file_path}")
    
    elapsed = 0
    while elapsed < timeout:
        if file_path.exists():
            logger.info(f"File found: {file_path}")
            return str(file_path)
        
        time.sleep(poke_interval)
        elapsed += poke_interval
        
        if elapsed % 300 == 0:
            logger.info(f"Still waiting... elapsed {elapsed}s")
    
    raise TimeoutError(f"File {file_path} not found after {timeout} seconds")


@task
def validate_schema(file_path: str) -> str:
    """
    Validate CSV schema: column names and data types.
    Expected columns: transaction_id (string), customer_id (string),
    amount (decimal), transaction_date (date).
    """
    logger = validate_schema.get_logger()
    logger.info(f"Validating schema for {file_path}")
    
    expected_columns = {
        "transaction_id": "string",
        "customer_id": "string",
        "amount": "decimal",
        "transaction_date": "date"
    }
    
    try:
        df = pd.read_csv(file_path, nrows=100)
        
        actual_columns = list(df.columns)
        expected_col_names = list(expected_columns.keys())
        
        if actual_columns != expected_col_names:
            raise ValueError(
                f"Column mismatch. Expected: {expected_col_names}, "
                f"Got: {actual_columns}"
            )
        
        try:
            pd.to_numeric(df["amount"])
        except Exception as e:
            raise ValueError(f"Amount column contains non-numeric values: {e}")
        
        try:
            pd.to_datetime(df["transaction_date"])
        except Exception as e:
            raise ValueError(f"Transaction_date column contains invalid dates: {e}")
        
        logger.info("Schema validation passed")
        return file_path
        
    except Exception as e:
        logger.error(f"Schema validation failed: {e}")
        raise ValueError(f"Schema validation failed: {e}")


@task
def load_db(
    file_path: str,
    host: str = "localhost",
    port: int = 5432,
    database: str = "postgres",
    table: str = "public.transactions",
) -> None:
    """
    Load validated CSV data into PostgreSQL.
    """
    logger = load_db.get_logger()
    logger.info(f"Loading data from {file_path} to {table}")
    
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    
    conn_params = {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
    }
    
    df = pd.read_csv(file_path)
    df["amount"] = pd.to_numeric(df["amount"])
    df["transaction_date"] = pd.to_datetime(df["transaction_date"])
    
    conn = None
    try:
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            transaction_id VARCHAR(255) PRIMARY KEY,
            customer_id VARCHAR(255),
            amount DECIMAL(10, 2),
            transaction_date DATE
        );
        """
        cur.execute(create_table_sql)
        
        from io import StringIO
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        
        cur.copy_expert(f"COPY {table} FROM STDIN WITH CSV", buffer)
        conn.commit()
        cur.close()
        
        logger.info(f"Successfully loaded {len(df)} rows to {table}")
        
    except Exception as e:
        logger.error(f"Failed to load data to PostgreSQL: {e}")
        raise RuntimeError(f"Failed to load data to PostgreSQL: {e}")
    finally:
        if conn:
            conn.close()


@flow(
    name="sensor-gated-daily-transactions",
    retries=2,
    retry_delay_seconds=300,
)
def daily_transactions_pipeline(
    base_dir: str = "/data/incoming",
    db_host: str = "localhost",
    db_port: int = 5432,
    db_name: str = "postgres",
    db_table: str = "public.transactions",
):
    """
    Daily transaction file processing pipeline.
    
    Linear sequence: wait_for_file → validate_schema → load_db
    
    Schedule: Daily execution via @daily interval
    Start Date: January 1, 2024
    Catchup: Disabled
    """
    file_path = wait_for_file(base_dir=base_dir)
    validated_path = validate_schema(file_path)
    load_db(
        validated_path,
        host=db_host,
        port=db_port,
        database=db_name,
        table=db_table,
    )


if __name__ == "__main__":
    # For local execution and testing
    # Deployment command for production:
    # prefect deployment build daily_transactions_pipeline.py:daily_transactions_pipeline \
    #   --name "daily-transactions" \
    #   --schedule "0 0 * * *" \
    #   --apply
    daily_transactions_pipeline()