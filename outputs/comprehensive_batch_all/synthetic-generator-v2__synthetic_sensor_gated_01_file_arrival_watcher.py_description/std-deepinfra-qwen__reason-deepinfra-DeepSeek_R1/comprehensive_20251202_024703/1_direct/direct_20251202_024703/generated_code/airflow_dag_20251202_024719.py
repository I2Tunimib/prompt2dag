from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),
}

def validate_schema(file_path):
    """
    Validates the schema of the CSV file.
    """
    required_columns = ['transaction_id', 'customer_id', 'amount', 'transaction_date']
    required_dtypes = {'transaction_id': str, 'customer_id': str, 'amount': float, 'transaction_date': str}
    
    df = pd.read_csv(file_path)
    
    # Check column names
    if not all(column in df.columns for column in required_columns):
        raise ValueError("Missing required columns in the file.")
    
    # Check data types
    for column, dtype in required_dtypes.items():
        if df[column].dtype != dtype:
            raise ValueError(f"Column {column} has incorrect data type.")
    
    # Check date format
    try:
        pd.to_datetime(df['transaction_date'], format='%Y-%m-%d')
    except ValueError:
        raise ValueError("Invalid date format in the transaction_date column.")
    
    print("Schema validation successful.")

def load_db(file_path):
    """
    Loads the validated data into the PostgreSQL database.
    """
    engine = create_engine('postgresql://user:password@localhost:5432/mydatabase')
    df = pd.read_csv(file_path)
    df.to_sql('transactions', engine, schema='public', if_exists='append', index=False)
    print("Data loaded to PostgreSQL successfully.")

with DAG(
    dag_id='daily_transaction_pipeline',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    description='Pipeline to monitor, validate, and load daily transaction files to PostgreSQL.',
) as dag:

    file_pattern = '/data/incoming/transactions_*.csv'
    
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath=file_pattern,
        poke_interval=30,
        timeout=86400,
        mode='poke',
    )
    
    validate_schema_task = PythonOperator(
        task_id='validate_schema',
        python_callable=validate_schema,
        op_kwargs={'file_path': file_pattern},
    )
    
    load_db_task = PythonOperator(
        task_id='load_db',
        python_callable=load_db,
        op_kwargs={'file_path': file_pattern},
    )
    
    wait_for_file >> validate_schema_task >> load_db_task