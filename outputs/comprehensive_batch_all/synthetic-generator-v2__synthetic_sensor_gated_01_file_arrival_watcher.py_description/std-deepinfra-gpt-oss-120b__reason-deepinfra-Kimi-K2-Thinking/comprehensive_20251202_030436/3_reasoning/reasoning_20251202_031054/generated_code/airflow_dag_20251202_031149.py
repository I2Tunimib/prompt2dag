from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import csv
import psycopg2
from psycopg2 import sql

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}


def validate_schema(**kwargs):
    """
    Validates the schema of the daily transaction file.
    Checks column names and data types.
    """
    execution_date = kwargs['ds_nodash']
    file_path = f"/data/incoming/transactions_{execution_date}.csv"
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    expected_columns = ['transaction_id', 'customer_id', 'amount', 'transaction_date']
    
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        actual_columns = reader.fieldnames
        
        if actual_columns != expected_columns:
            raise ValueError(
                f"Column mismatch. Expected: {expected_columns}, Got: {actual_columns}"
            )
        
        for row_num, row in enumerate(reader, start=1):
            try:
                # Validate transaction_id (string)
                if not isinstance(row['transaction_id'], str) or not row['transaction_id']:
                    raise ValueError("transaction_id must be a non-empty string")
                
                # Validate customer_id (string)
                if not isinstance(row['customer_id'], str) or not row['customer_id']:
                    raise ValueError("customer_id must be a non-empty string")
                
                # Validate amount (decimal)
                float(row['amount'])
                
                # Validate transaction_date (date)
                datetime.strptime(row['transaction_date'], '%Y-%m-%d')
                
            except (ValueError, KeyError, TypeError) as e:
                raise ValueError(f"Data validation failed at row {row_num}: {e}")


def load_to_postgres(**kwargs):
    """
    Loads validated transaction data to PostgreSQL.
    """
    execution_date = kwargs['ds_nodash']
    file_path = f"/data/incoming/transactions_{execution_date}.csv"
    
    # In production, use Airflow Connections instead of hardcoded values
    conn_params = {
        'host': 'localhost',
        'port': 5432,
        'database': 'airflow',
        'user': 'airflow',
        'password': 'airflow'
    }
    
    conn = psycopg2.connect(**conn_params)
    cursor = conn.cursor()
    
    try:
        with open(file_path, 'r') as f:
            next(f)  # Skip header
            cursor.copy_expert(
                sql.SQL("""
                    COPY public.transactions (transaction_id, customer_id, amount, transaction_date) 
                    FROM STDIN WITH CSV
                """),
                f
            )
        
        conn.commit()
        print(f"Successfully loaded {cursor.rowcount} rows to public.transactions")
        
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


with DAG(
    dag_id='daily_transaction_pipeline',
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    tags=['transaction', 'daily', 'sensor-gated'],
) as dag:
    
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath="/data/incoming/transactions_{{ ds_nodash }}.csv",
        fs_conn_id='fs_default',  # Configure this connection in Airflow UI
        poke_interval=30,
        timeout=86400,  # 24 hours in seconds
        mode='poke',
    )
    
    validate_schema_task = PythonOperator(
        task_id='validate_schema',
        python_callable=validate_schema,
    )
    
    load_db_task = PythonOperator(
        task_id='load_db',
        python_callable=load_to_postgres,
    )
    
    wait_for_file >> validate_schema_task >> load_db_task