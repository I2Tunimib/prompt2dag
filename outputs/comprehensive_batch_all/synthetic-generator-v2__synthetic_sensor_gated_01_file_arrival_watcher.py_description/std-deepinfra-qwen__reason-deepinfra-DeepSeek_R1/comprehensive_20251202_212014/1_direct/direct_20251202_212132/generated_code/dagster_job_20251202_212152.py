from datetime import datetime, timedelta
from dagster import op, job, sensor, RunRequest, RunConfig, resource, RetryPolicy, daily_schedule
import os
import pandas as pd
from sqlalchemy import create_engine


@resource
def postgres_resource():
    engine = create_engine('postgresql://localhost:5432')
    return engine


@op(required_resource_keys={"postgres"})
def load_db(context, file_path: str):
    """
    Load the validated data to PostgreSQL.
    """
    df = pd.read_csv(file_path)
    df.to_sql('transactions', context.resources.postgres, schema='public', if_exists='append', index=False)


@op
def validate_schema(context, file_path: str):
    """
    Validate the schema of the CSV file.
    """
    required_columns = ['transaction_id', 'customer_id', 'amount', 'transaction_date']
    required_dtypes = {'transaction_id': str, 'customer_id': str, 'amount': float, 'transaction_date': str}

    df = pd.read_csv(file_path)
    if not all(column in df.columns for column in required_columns):
        raise ValueError("Missing required columns")

    for column, dtype in required_dtypes.items():
        if df[column].dtype != dtype:
            raise ValueError(f"Column {column} has incorrect data type")

    return file_path


@op
def wait_for_file(context):
    """
    Monitor the /data/incoming directory for a file matching the pattern transactions_YYYYMMDD.csv.
    """
    directory = '/data/incoming'
    pattern = 'transactions_*.csv'

    for _ in range(2880):  # 24 hours * 60 minutes / 30 seconds per poke
        for file_name in os.listdir(directory):
            if file_name.startswith('transactions_') and file_name.endswith('.csv'):
                file_path = os.path.join(directory, file_name)
                return file_path
        context.log.info("File not found, waiting 30 seconds...")
        context.sleep(30)

    raise FileNotFoundError("File not found within 24 hours")


@job(resource_defs={"postgres": postgres_resource}, retry_policy=RetryPolicy(max_retries=2, delay=300))
def transaction_pipeline():
    file_path = wait_for_file()
    validated_file_path = validate_schema(file_path)
    load_db(validated_file_path)


@daily_schedule(
    pipeline_name="transaction_pipeline",
    start_date=datetime(2024, 1, 1),
    execution_time=datetime.time(hour=0, minute=0),
    execution_timezone="UTC",
    tags={"dagster/is_partitioned": "true"},
    solid_selection=None,
    mode="default",
    should_execute=None,
    environment_vars=None,
    partition_days_offset=0,
    end_date=None,
    cron_schedule="0 0 * * *",
    execution_timezone="UTC",
    name="daily_transaction_pipeline",
    description="Daily pipeline to process transaction files.",
)
def daily_transaction_pipeline(_date):
    return RunConfig()


@sensor(job=transaction_pipeline, minimum_interval_seconds=30)
def file_sensor(context):
    directory = '/data/incoming'
    pattern = 'transactions_*.csv'

    for file_name in os.listdir(directory):
        if file_name.startswith('transactions_') and file_name.endswith('.csv'):
            file_path = os.path.join(directory, file_name)
            yield RunRequest(run_key=file_name, run_config={"ops": {"wait_for_file": {"config": {"file_path": file_path}}}})


if __name__ == '__main__':
    result = transaction_pipeline.execute_in_process()