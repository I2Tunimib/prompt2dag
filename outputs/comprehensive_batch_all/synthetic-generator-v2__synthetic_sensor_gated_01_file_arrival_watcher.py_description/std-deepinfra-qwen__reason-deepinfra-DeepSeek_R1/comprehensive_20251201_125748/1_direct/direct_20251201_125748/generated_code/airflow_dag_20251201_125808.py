from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import os
import pandas as pd
import psycopg2
from psycopg2 import sql

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def validate_schema(file_path):
    """
    Validates the schema of the CSV file.
    """
    required_columns = ['transaction_id', 'customer_id', 'amount', 'transaction_date']
    required_dtypes = {'transaction_id': str, 'customer_id': str, 'amount': float, 'transaction_date': 'datetime64'}

    df = pd.read_csv(file_path)
    if not all(column in df.columns for column in required_columns):
        raise ValueError("Missing required columns")

    for column, dtype in required_dtypes.items():
        if df[column].dtype != dtype:
            raise ValueError(f"Column {column} has incorrect data type")

def load_db(file_path):
    """
    Loads the validated data into the PostgreSQL database.
    """
    conn = psycopg2.connect(
        dbname="your_db",
        user="your_user",
        password="your_password",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    with open(file_path, 'r') as f:
        next(f)  # Skip the header row
        cur.copy_from(f, 'public.transactions', sep=',')

    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id='daily_transaction_pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
) as dag:

    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath='/data/incoming/transactions_{{ ds_nodash }}.csv',
        poke_interval=30,
        timeout=86400,
        fs_conn_id='fs_default',
    )

    validate_schema_task = PythonOperator(
        task_id='validate_schema',
        python_callable=validate_schema,
        op_kwargs={'file_path': '/data/incoming/transactions_{{ ds_nodash }}.csv'},
    )

    load_db_task = PythonOperator(
        task_id='load_db',
        python_callable=load_db,
        op_kwargs={'file_path': '/data/incoming/transactions_{{ ds_nodash }}.csv'},
    )

    wait_for_file >> validate_schema_task >> load_db_task