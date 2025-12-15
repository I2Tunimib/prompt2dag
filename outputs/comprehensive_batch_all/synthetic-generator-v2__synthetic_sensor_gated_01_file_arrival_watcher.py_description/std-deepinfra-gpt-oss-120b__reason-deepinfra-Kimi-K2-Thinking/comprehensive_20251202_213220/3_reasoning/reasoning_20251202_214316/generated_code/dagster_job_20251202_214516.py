import os
import time
import csv
import psycopg2
from datetime import datetime
from typing import List, Dict, Any

from dagster import (
    op,
    job,
    Config,
    OpExecutionContext,
    ScheduleDefinition,
    Definitions,
    RetryPolicy,
    DefaultScheduleStatus,
)


class FileSensorConfig(Config):
    directory: str = "/data/incoming"
    file_pattern: str = "transactions_{date}.csv"
    poke_interval: int = 30
    timeout: int = 86400  # 24 hours in seconds


class DatabaseConfig(Config):
    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    user: str = "postgres"
    password: str = "postgres"
    table: str = "public.transactions"


@op(
    description="Monitors directory for transaction file arrival with date-based pattern",
    config_schema=FileSensorConfig,
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def wait_for_file_op(context: OpExecutionContext, config: FileSensorConfig) -> str:
    """
    Waits for a file matching the pattern to appear in the specified directory.
    Pokes every 30 seconds for up to 24 hours.
    """
    # Get execution date from context
    run_date = context.partition_key if context.has_partition_key else datetime.now().strftime("%Y%m%d")
    
    # Format the expected filename
    expected_filename = config.file_pattern.format(date=run_date)
    expected_path = os.path.join(config.directory, expected_filename)
    
    context.log.info(f"Waiting for file: {expected_path}")
    
    elapsed = 0
    while elapsed < config.timeout:
        if os.path.exists(expected_path):
            context.log.info(f"File found: {expected_path}")
            return expected_path
        
        time.sleep(config.poke_interval)
        elapsed += config.poke_interval
        
        if elapsed % 300 == 0:  # Log every 5 minutes
            context.log.info(f"Still waiting... elapsed {elapsed}s")
    
    raise TimeoutError(f"File not found after {config.timeout} seconds: {expected_path}")


@op(
    description="Validates schema of the transaction file",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def validate_schema_op(context: OpExecutionContext, file_path: str) -> bool:
    """
    Validates column names and data types in the transaction file.
    Expected columns: transaction_id (string), customer_id (string), 
    amount (decimal), transaction_date (date)
    """
    expected_columns = ["transaction_id", "customer_id", "amount", "transaction_date"]
    
    try:
        with open(file_path, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            
            # Validate column names
            actual_columns = reader.fieldnames or []
            if actual_columns != expected_columns:
                raise ValueError(
                    f"Column mismatch. Expected: {expected_columns}, Got: {actual_columns}"
                )
            
            # Validate data types for first few rows
            row_count = 0
            for row in reader:
                row_count += 1
                
                # Validate transaction_id (string)
                if not isinstance(row["transaction_id"], str):
                    raise ValueError(f"transaction_id must be string, got {type(row['transaction_id'])}")
                
                # Validate customer_id (string)
                if not isinstance(row["customer_id"], str):
                    raise ValueError(f"customer_id must be string, got {type(row['customer_id'])}")
                
                # Validate amount (decimal)
                try:
                    float(row["amount"])
                except (ValueError, TypeError):
                    raise ValueError(f"amount must be decimal, got {row['amount']}")
                
                # Validate transaction_date (date)
                try:
                    datetime.strptime(row["transaction_date"], "%Y-%m-%d")
                except (ValueError, TypeError):
                    raise ValueError(
                        f"transaction_date must be in YYYY-MM-DD format, got {row['transaction_date']}"
                    )
                
                # Only validate first 10 rows for performance
                if row_count >= 10:
                    break
        
        context.log.info(f"Schema validation passed for {file_path}")
        return True
        
    except Exception as e:
        context.log.error(f"Schema validation failed: {str(e)}")
        raise


@op(
    description="Loads validated data to PostgreSQL",
    config_schema=DatabaseConfig,
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def load_db_op(context: OpExecutionContext, file_path: str, validation_result: bool) -> None:
    """
    Loads data from CSV file to PostgreSQL transactions table.
    """
    if not validation_result:
        raise ValueError("Cannot load data that failed validation")
    
    config = context.op_config
    
    conn = None
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password,
        )
        cursor = conn.cursor()
        
        # Use COPY command for efficient bulk loading
        with open(file_path, 'r') as f:
            # Skip header row
            next(f)
            cursor.copy_expert(
                f"COPY {config.table} (transaction_id, customer_id, amount, transaction_date) FROM STDIN WITH CSV",
                f
            )
        
        conn.commit()
        row_count = cursor.rowcount
        context.log.info(f"Successfully loaded {row_count} rows to {config.table}")
        
    except Exception as e:
        context.log.error(f"Failed to load data to PostgreSQL: {str(e)}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()


@job(
    description="Daily transaction file processing pipeline with sensor gating",
)
def daily_transaction_pipeline():
    """
    Linear pipeline: wait_for_file -> validate_schema -> load_db
    """
    file_path = wait_for_file_op()
    validation_result = validate_schema_op(file_path)
    load_db_op(file_path, validation_result)


# Schedule definition
daily_schedule = ScheduleDefinition(
    job=daily_transaction_pipeline,
    cron_schedule="@daily",
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
)


# Definitions object to tie everything together
defs = Definitions(
    jobs=[daily_transaction_pipeline],
    schedules=[daily_schedule],
)


if __name__ == "__main__":
    # Example execution for a specific date
    result = daily_transaction_pipeline.execute_in_process(
        run_config={
            "ops": {
                "wait_for_file_op": {
                    "config": {
                        "directory": "/data/incoming",
                        "file_pattern": "transactions_{date}.csv",
                        "poke_interval": 30,
                        "timeout": 86400,
                    }
                },
                "load_db_op": {
                    "config": {
                        "host": "localhost",
                        "port": 5432,
                        "database": "postgres",
                        "user": "postgres",
                        "password": "postgres",
                        "table": "public.transactions",
                    }
                }
            }
        },
        partition_key="20240101",  # Example date
    )
    
    if result.success:
        print("Pipeline execution succeeded!")
    else:
        print("Pipeline execution failed!")
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                print(f"Step failed: {event.step_key}")