import csv
import datetime
import decimal
from pathlib import Path

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta


def validate_schema(**kwargs):
    """
    Validates the schema of the transaction file.
    Checks column names and data types.
    """
    execution_date = kwargs['ds_nodash']
    file_path = f"/data/incoming/transactions_{execution_date}.csv"
    
    expected_columns = ['transaction_id', 'customer_id', 'amount', 'transaction_date']
    
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    with open(file_path, 'r', newline='') as f:
        reader = csv.DictReader(f)
        actual_columns = reader.fieldnames
        
        if actual_columns != expected_columns:
            raise ValueError(f"Column mismatch. Expected: {expected_columns}, Got: {actual_columns}")
        
        for i, row in enumerate(reader):
            if i >= 10:
                break
                
            try:
                if not row['transaction_id'] or not isinstance(row['transaction_id'], str):
                    raise ValueError(f"Invalid transaction_id: {row['transaction_id']}")
                
                if not row['customer_id'] or not isinstance(row['customer_id'], str):
                    raise ValueError(f"Invalid customer_id: {row['customer_id']}")
                
                decimal.Decimal(row['amount'])
                datetime.datetime.strptime(row['transaction_date'], '%Y-%m-%d').date()
                
            except (ValueError, decimal.InvalidOperation, KeyError) as e:
                raise ValueError(f"Schema validation failed at row {i+2}: {e}")


def load_to_postgres(**kwargs):
    """
    Loads validated transaction data to PostgreSQL.
    """
    execution_date = kwargs['ds_nodash']
    file_path = f"/data/incoming/transactions_{execution_date}.csv"
    
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_local')
    conn = pg_hook.get_conn()
    
    try:
        with conn.cursor() as cursor:
            with open(file_path, 'r', newline='') as f:
                reader = csv.DictReader(f)
                
                insert_sql = """
                    INSERT INTO public.transactions 
                    (transaction_id, customer_id, amount, transaction_date)
                    VALUES (%s, %s, %s, %s)
                """
                
                batch = []
                for row in reader:
                    batch.append((
                        row['transaction_id'],
                        row['customer_id'],
                        decimal.Decimal(row['amount']),
                        row['transaction_date']
                    ))
                    
                    if len(batch) >= 1000:
                        cursor.executemany(insert_sql, batch)
                        batch = []
                
                if batch:
                    cursor.executemany(insert_sql, batch)
            
            conn.commit()
            
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_transaction_pipeline',
    schedule='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=['transactions', 'daily'],
) as dag:
    
    wait_for_file = FileSensor(
        task_id='wait_for_file',
        filepath='/data/incoming/transactions_{{ ds_nodash }}.csv',
        fs_conn_id='fs_local',
        poke_interval=30,
        timeout=24 * 60 * 60,
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