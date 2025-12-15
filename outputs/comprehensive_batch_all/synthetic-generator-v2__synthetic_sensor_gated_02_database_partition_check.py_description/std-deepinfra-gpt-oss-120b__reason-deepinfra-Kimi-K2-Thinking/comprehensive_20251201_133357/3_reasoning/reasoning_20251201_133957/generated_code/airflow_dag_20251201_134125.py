from datetime import datetime, timedelta
import logging

import pandas as pd
import sqlalchemy
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.sensors.sql import SqlSensor

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def extract_incremental(**context):
    """Extract new orders data from the orders table for the current date partition."""
    execution_date = context['execution_date']
    partition_date = execution_date.strftime('%Y-%m-%d')
    
    conn = BaseHook.get_connection('database_conn')
    
    sql = f"""
    SELECT 
        order_id,
        customer_id,
        customer_name,
        customer_email,
        order_amount,
        quantity,
        order_timestamp,
        status
    FROM orders
    WHERE DATE(order_timestamp) = '{partition_date}'
    """
    
    logger.info(f"Extracting orders for partition date: {partition_date}")
    
    engine = sqlalchemy.create_engine(conn.get_uri())
    df = pd.read_sql(sql, engine)
    
    context['task_instance'].xcom_push(key='orders_data', value=df.to_dict('records'))
    logger.info(f"Extracted {len(df)} orders")


def transform(**context):
    """Transform extracted orders data: clean customer info, validate amounts/quantities,
    and format timestamps to ISO standard."""
    ti = context['task_instance']
    records = ti.xcom_pull(key='orders_data', task_ids='extract_incremental')
    
    if not records:
        logger.warning("No data to transform")
        ti.xcom_push(key='transformed_data', value=[])
        return
    
    df = pd.DataFrame(records)
    
    df['customer_name'] = df['customer_name'].str.strip().str.title()
    df['customer_email'] = df['customer_email'].str.strip().lower()
    
    df = df[(df['order_amount'] > 0) & (df['quantity'] > 0)]
    
    df['order_timestamp'] = pd.to_datetime(df['order_timestamp']).dt.isoformat()
    
    df['processed_at'] = datetime.utcnow().isoformat()
    
    ti.xcom_push(key='transformed_data', value=df.to_dict('records'))
    logger.info(f"Transformed {len(df)} valid orders")


def load(**context):
    """Load transformed records into fact_orders table in data warehouse
    and update related metrics and aggregates."""
    ti = context['task_instance']
    records = ti.xcom_pull(key='transformed_data', task_ids='transform')
    
    if not records:
        logger.warning("No data to load")
        return
    
    df = pd.DataFrame(records)
    conn = BaseHook.get_connection('database_conn')
    engine = sqlalchemy.create_engine(conn.get_uri())
    
    table_name = 'fact_orders'
    temp_table = f'{table_name}_temp'
    
    try:
        df.to_sql(temp_table, engine, if_exists='replace', index=False)
        
        upsert_sql = f"""
        INSERT INTO {table_name} (
            order_id, customer_id, customer_name, customer_email, 
            order_amount, quantity, order_timestamp, status, processed_at
        )
        SELECT 
            order_id, customer_id, customer_name, customer_email,
            order_amount, quantity, order_timestamp, status, processed_at
        FROM {temp_table}
        ON CONFLICT (order_id) 
        DO UPDATE SET
            customer_name = EXCLUDED.customer_name,
            customer_email = EXCLUDED.customer_email,
            order_amount = EXCLUDED.order_amount,
            quantity = EXCLUDED.quantity,
            order_timestamp = EXCLUDED.order_timestamp,
            status = EXCLUDED.status,
            processed_at = EXCLUDED.processed_at;
        """
        
        with engine.begin() as connection:
            connection.execute(upsert_sql)
            
            metrics_sql = f"""
            INSERT INTO daily_order_metrics (order_date, total_orders, total_amount)
            SELECT 
                DATE(order_timestamp), 
                COUNT(*), 
                SUM(order_amount)
            FROM {table_name}
            WHERE DATE(order_timestamp) = '{context["execution_date"].strftime("%Y-%m-%d")}'
            GROUP BY DATE(order_timestamp)
            ON CONFLICT (order_date) 
            DO UPDATE SET
                total_orders = EXCLUDED.total_orders,
                total_amount = EXCLUDED.total_amount;
            """
            connection.execute(metrics_sql)
            
        logger.info("Load completed successfully")
        
    finally:
        with engine.begin() as connection:
            connection.execute(f"DROP TABLE IF EXISTS {temp_table}")


with DAG(
    dag_id='daily_orders_etl',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    description='Daily ETL pipeline for incremental orders processing',
    tags=['etl', 'orders', 'daily'],
) as dag:
    
    wait_partition = SqlSensor(
        task_id='wait_partition',
        conn_id='database_conn',
        sql="""
        SELECT 1 
        FROM information_schema.partitions 
        WHERE table_schema = 'public' 
          AND table_name = 'orders' 
          AND partition_description = '{{ ds }}'
        """,
        mode='reschedule',
        poke_interval=300,
        timeout=3600,
    )
    
    extract_incremental = PythonOperator(
        task_id='extract_incremental',
        python_callable=extract_incremental,
    )
    
    transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )
    
    load = PythonOperator(
        task_id='load',
        python_callable=load,
    )
    
    wait_partition >> extract_incremental >> transform >> load