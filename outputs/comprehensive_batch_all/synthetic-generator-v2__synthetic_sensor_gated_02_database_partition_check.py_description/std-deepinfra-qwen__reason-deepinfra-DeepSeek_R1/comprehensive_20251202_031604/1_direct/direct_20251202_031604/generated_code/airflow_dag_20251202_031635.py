from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def extract_incremental(**kwargs):
    """
    Extract new orders data from the orders table filtered by the current date partition.
    """
    # Example SQL query to extract data
    sql = """
    SELECT * FROM orders WHERE partition_date = '{{ ds }}'
    """
    # Execute the SQL query and fetch results
    # This is a placeholder for actual database interaction
    logging.info(f"Extracting data for date: {kwargs['ds']}")
    # Example data extraction
    data = [{'order_id': 1, 'customer_id': 101, 'amount': 100.0, 'quantity': 2, 'timestamp': '2024-01-01T12:00:00Z'}]
    return data

def transform(**kwargs):
    """
    Process the extracted orders data by cleaning customer information, validating order amounts and quantities,
    and formatting timestamps to ISO standard.
    """
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_incremental')
    transformed_data = []
    for record in data:
        # Example transformations
        record['customer_id'] = str(record['customer_id']).strip()
        record['amount'] = round(record['amount'], 2)
        record['quantity'] = int(record['quantity'])
        record['timestamp'] = record['timestamp'].replace(' ', 'T') + 'Z'
        transformed_data.append(record)
    return transformed_data

def load(**kwargs):
    """
    Upsert the transformed records into the fact_orders table in the data warehouse and update related metrics and aggregates.
    """
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform')
    # Example SQL upsert query
    sql = """
    INSERT INTO fact_orders (order_id, customer_id, amount, quantity, timestamp)
    VALUES (%(order_id)s, %(customer_id)s, %(amount)s, %(quantity)s, %(timestamp)s)
    ON CONFLICT (order_id) DO UPDATE SET
    customer_id = EXCLUDED.customer_id,
    amount = EXCLUDED.amount,
    quantity = EXCLUDED.quantity,
    timestamp = EXCLUDED.timestamp
    """
    # Execute the SQL upsert query
    # This is a placeholder for actual database interaction
    logging.info(f"Loading data: {data}")

with DAG(
    dag_id='daily_orders_etl',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    description='Daily ETL pipeline for processing incremental orders data',
) as dag:

    wait_partition = SqlSensor(
        task_id='wait_partition',
        conn_id='database_conn',
        sql="SELECT 1 FROM information_schema.tables WHERE table_name = 'orders' AND partition_date = '{{ ds }}'",
        mode='reschedule',
        poke_interval=300,
        timeout=3600,
        dag=dag,
    )

    extract_incremental = PythonOperator(
        task_id='extract_incremental',
        python_callable=extract_incremental,
        provide_context=True,
        dag=dag,
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
        provide_context=True,
        dag=dag,
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load,
        provide_context=True,
        dag=dag,
    )

    wait_partition >> extract_incremental >> transform >> load