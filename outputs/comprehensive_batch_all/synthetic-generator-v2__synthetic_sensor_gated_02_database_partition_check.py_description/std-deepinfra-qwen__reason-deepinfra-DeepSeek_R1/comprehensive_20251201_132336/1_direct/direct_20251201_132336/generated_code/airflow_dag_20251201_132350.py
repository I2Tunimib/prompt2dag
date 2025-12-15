from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.sql import SqlSensor
from airflow.utils.dates import days_ago
from datetime import timedelta
import pendulum

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.datetime(2024, 1, 1, tz="UTC"),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

def extract_incremental(**kwargs):
    """Extract new orders data from the orders table filtered by the current date partition."""
    # Placeholder for actual extraction logic
    pass

def transform(**kwargs):
    """Process the extracted orders data by cleaning, validating, and formatting."""
    # Placeholder for actual transformation logic
    pass

def load(**kwargs):
    """Upsert the transformed records into the fact_orders table in the data warehouse."""
    # Placeholder for actual loading logic
    pass

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
        sql="SELECT * FROM information_schema.tables WHERE table_name = 'orders' AND table_schema = 'public' AND partition_name = 'orders_{{ ds }}';",
        mode='reschedule',
        poke_interval=300,
        timeout=3600,
    )

    extract_incremental_task = PythonOperator(
        task_id='extract_incremental',
        python_callable=extract_incremental,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load,
        provide_context=True,
    )

    wait_partition >> extract_incremental_task >> transform_task >> load_task