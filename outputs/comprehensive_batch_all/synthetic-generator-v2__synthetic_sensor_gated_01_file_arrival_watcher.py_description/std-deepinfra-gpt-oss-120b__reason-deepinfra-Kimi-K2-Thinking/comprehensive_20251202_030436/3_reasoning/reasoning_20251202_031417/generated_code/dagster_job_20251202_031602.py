import os
import time
import csv
from datetime import datetime
from typing import List, Any
import psycopg2
from psycopg2.extras import execute_batch

from dagster import (
    op,
    job,
    ScheduleDefinition,
    Definitions,
    RetryPolicy,
    OpExecutionContext,
    AssetMaterialization,
    MetadataValue,
    In,
    Out,
    Nothing,
    DefaultScheduleStatus,
)

# Configuration - override via environment variables
BASE_DIR = os.getenv("TRANSACTIONS_BASE_DIR", "/data/incoming")
FILE_PATTERN = "transactions_{date_str}.csv"
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "transactions_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
TARGET_TABLE = "public.transactions"

# Sensor polling configuration
POKE_INTERVAL = 30
TIMEOUT_SECONDS = 24 * 60 * 60

# Shared retry policy
RETRY_POLICY = RetryPolicy(max_retries=2, delay=300)


@op(
    retry_policy=RETRY_POLICY,
    out=Out(str, description="Path to the transaction file"),
)
def wait_for_file(context: OpExecutionContext) -> str:
    """Monitors /data/incoming for the daily transaction file."""
    scheduled_time = context.pipeline_run.execution_time
    if scheduled_time is None:
        scheduled_time = datetime.now()
    
    run_date = scheduled_time.strftime("%Y%m%d")
    file_path = os.path.join(BASE_DIR, FILE_PATTERN.format(date_str=run_date))
    
    context.log.info(f"Waiting for file: {file_path}")
    
    elapsed = 0
    while elapsed < TIMEOUT_SECONDS:
        if os.path.exists(file_path):
            context.log.info(f"File found: {file_path}")
            context.log_event(
                AssetMaterialization(
                    asset_key="daily_transaction_file",
                    description="Daily transaction CSV file",
                    metadata={
                        "file_path": MetadataValue.path(file_path),
                        "file_size": MetadataValue.int(os.path.getsize(file_path)),
                    },
                )
            )
            return file_path
        
        time.sleep(POKE_INTERVAL)
        elapsed += POKE_INTERVAL
        if elapsed % 300 == 0:
            context.log.info(f"File not found yet. Elapsed: {elapsed}s")
    
    raise FileNotFoundError(
        f"Timeout after {TIMEOUT_SECONDS}s waiting for file: {file_path}"
    )


@op(
    ins={"file_path": In(str, description="Path to the transaction file")},
    retry_policy=RETRY_POLICY,
    out=Out(str, description="Validated file path"),
)
def validate_schema(context: OpExecutionContext, file_path: str) -> str:
    """Validates column names and data types in the transaction file."""
    context.log.info(f"Validating schema for file: {file_path}")
    
    expected_columns = ["transaction_id", "customer_id", "amount", "transaction_date"]
    expected_types = ["string", "string", "decimal", "date"]
    
    try:
        with open(file_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            
            if not header:
                raise ValueError("File is empty")
            
            header = [col.strip() for col in header]
            if header != expected_columns:
                raise ValueError(
                    f"Column mismatch. Expected: {expected_columns}, Got: {header}"
                )
            
            # Sample rows for type validation
            sample_rows = [row for i, row in enumerate(reader) if i < 5 and row]
            
            if not sample_rows:
                context.log.warning("No data rows for type validation")
                return file_path
            
            for row_idx, row in enumerate(sample_rows):
                if len(row) != len(expected_columns):
                    raise ValueError(
                        f"Row {row_idx + 1} has {len(row)} columns, "
                        f"expected {len(expected_columns)}"
                    )
                
                for col_idx, (value, expected_type) in enumerate(zip(row, expected_types)):
                    col_name = expected_columns[col_idx]
                    
                    if expected_type == "decimal":
                        try:
                            float(value)
                        except ValueError:
                            raise ValueError(
                                f"Row {row_idx + 1}, column '{col_name}': "
                                f"Expected decimal, got '{value}'