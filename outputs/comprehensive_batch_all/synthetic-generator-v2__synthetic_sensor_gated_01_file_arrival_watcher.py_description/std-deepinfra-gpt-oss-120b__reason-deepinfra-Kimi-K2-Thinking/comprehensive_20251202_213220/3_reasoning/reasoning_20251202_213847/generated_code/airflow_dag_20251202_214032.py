import csv
import datetime
import decimal
from datetime import timedelta

import psycopg2
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor


def validate_schema(file_path: str) -> None:
    """Validate CSV schema and data types for transaction file."""
    expected_columns = ['transaction_id', 'customer_id', 'amount', 'transaction_date']
    expected_date_format = '%Y-%m-%d'
    
    with open(file_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        if set(reader.fieldnames) != set(expected_columns):
            raise ValueError(
                f"Column mismatch. Expected: {expected_columns}, Got: {reader.fieldnames}"
            )
        
        for row_num, row in enumerate(reader, start=1):
            try:
                if not str(row['transaction_id']).strip():
                    raise ValueError("transaction_id cannot be empty")
                
                if not str(row['customer_id']).strip():
                    raise ValueError("customer_id cannot be empty")
                
                amount = decimal.Decimal(row['amount'])
                if amount < 0:
                    raise ValueError("amount cannot be negative")
                
                datetime.datetime.strptime(row['transaction_date'], expected_date_format)
                
            except (ValueError, decimal.InvalidOperation, KeyError) as e:
                raise ValueError(f"Data validation failed at row {row_num}: {e}")


def load_to_postgres(file_path: str) -> None:
    """Load validated CSV data to PostgreSQL transactions table."""
    conn_id = 'postgres_default'
    
    try:
        connection = BaseHook.get_connection(conn_id)
        conn_params = {
            'host': connection.host or 'localhost',
            'port': connection.port or 5432,
            'database': connection.schema or 'transactions_db',
            'user': connection.login or 'airflow',
            'password': connection.password or 'airflow'
        }
    except Exception:
        conn_params = {
            'host': 'localhost',
            'port': 5432,
            'database': 'transactions_db',
            'user': 'airflow',
            'password': 'airflow'
        }
    
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS public.transactions (
            transaction_id VARCHAR(255) PRIMARY KEY,
            customer_id VARCHAR(255) NOT NULL,
            amount DECIMAL(15, 2) NOT NULL,
            transaction_date DATE NOT NULL
        );
    """
    
    insert_sql = """
        INSERT INTO public.transactions (transaction_id, customer_id, amount, transaction_date)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (transaction_id) DO UPDATE SET
            customer_id = EXCLUDED.customer_id,
            amount = EXCLUDED.amount,
            transaction_date = EXCLUDED.transaction_date;
    """
    
    conn = psycopg2.connect(**conn_params)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(create_table_sql)
                
                with open(file_path, 'r', newline='', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    rows = [
                        (
                            row['transaction_id'].strip(),
                            row['customer_id'].strip(),
                            decimal.Decimal(row['amount']),
                            row['transaction_date']
                        )
                        for row in reader
                    ]
                    
                    if rows:
                        cur.executemany(insert_sql, rows)
    finally:
        conn.close()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime.datetime(2024, 1, 1),
}

with DAG(
    dag_id='daily_transaction_pipeline',
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    description='Sensor-gated pipeline for daily transaction file processing',
    tags=['transactions', 'daily'],
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
        python_callable=load_to_postgres,
        op_kwargs={'file_path': '/data/incoming/transactions_{{ ds_nodash }}.csv'},
    )
    
    wait_for_file >> validate_schema_task >> load_db_task