from dagster import op, job, sensor, RunRequest, RunStatusSensorContext, RunStatus, In, Out, RetryPolicy, daily_schedule, Field, String
import os
import pandas as pd
from datetime import datetime
import psycopg2
from psycopg2 import sql

# Resources
class PostgresResource:
    def __init__(self, host, port, user, password, dbname):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.dbname = dbname

    def get_connection(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.dbname
        )

# Ops
@op(
    required_resource_keys={"postgres"},
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def load_db(context, file_path: str):
    """Loads the validated data to PostgreSQL."""
    conn = context.resources.postgres.get_connection()
    cursor = conn.cursor()
    with open(file_path, 'r') as file:
        cursor.copy_expert(
            sql.SQL("COPY public.transactions FROM STDIN WITH CSV HEADER"),
            file
        )
    conn.commit()
    cursor.close()
    conn.close()

@op
def validate_schema(context, file_path: str):
    """Validates the schema of the CSV file."""
    df = pd.read_csv(file_path)
    expected_columns = ['transaction_id', 'customer_id', 'amount', 'transaction_date']
    expected_dtypes = {'transaction_id': str, 'customer_id': str, 'amount': float, 'transaction_date': 'datetime64[ns]'}
    
    if not all(column in df.columns for column in expected_columns):
        raise ValueError("Missing required columns")
    
    for column, dtype in expected_dtypes.items():
        if df[column].dtype != dtype:
            raise ValueError(f"Column {column} has incorrect data type")
    
    return file_path

@op
def wait_for_file(context):
    """Waits for a file to appear in the /data/incoming directory."""
    directory = '/data/incoming'
    pattern = 'transactions_*.csv'
    
    for _ in range(2880):  # 24 hours * 60 minutes / 30 seconds per check
        files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f)) and fnmatch.fnmatch(f, pattern)]
        if files:
            return os.path.join(directory, files[0])
        time.sleep(30)
    
    raise FileNotFoundError("No file found within 24 hours")

# Job
@job(
    resource_defs={
        "postgres": PostgresResource(host="localhost", port="5432", user="user", password="password", dbname="dbname")
    }
)
def transaction_pipeline():
    file_path = wait_for_file()
    validated_file_path = validate_schema(file_path)
    load_db(validated_file_path)

# Schedule
@daily_schedule(
    pipeline_name="transaction_pipeline",
    start_date=datetime(2024, 1, 1),
    execution_time=datetime.time(hour=0, minute=0),
    execution_timezone="UTC",
    catchup=False
)
def daily_transaction_pipeline_schedule(date):
    return RunRequest(run_key=None, run_config={})

# Sensor
@sensor(job=transaction_pipeline)
def file_sensor(context):
    directory = '/data/incoming'
    pattern = 'transactions_*.csv'
    files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f)) and fnmatch.fnmatch(f, pattern)]
    if files:
        return RunRequest(run_key=None, run_config={})

if __name__ == '__main__':
    result = transaction_pipeline.execute_in_process()