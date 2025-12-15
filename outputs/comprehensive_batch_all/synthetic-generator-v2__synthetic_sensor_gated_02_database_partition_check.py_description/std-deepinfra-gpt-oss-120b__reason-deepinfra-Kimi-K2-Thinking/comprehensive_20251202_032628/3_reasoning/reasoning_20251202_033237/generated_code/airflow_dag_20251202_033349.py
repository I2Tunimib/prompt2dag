from datetime import datetime, timedelta
from typing import Dict, List, Any

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from airflow.sensors.sql import SqlSensor

# Required packages: pandas, sqlalchemy
import pandas as pd
import sqlalchemy


default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def extract_incremental_orders(**context) -> str:
    """
    Extracts new orders data from the orders table for the current date partition.
    """
    execution_date = context['logical_date']
    partition_date = execution_date.strftime('%Y-%m-%d')
    
    try:
        conn = BaseHook.get_connection('database_conn')
        engine = sqlalchemy.create_engine(conn.get_uri())
        
        query = f"""
        SELECT 
            order_id,
            customer_name,
            customer_email,
            order_amount,
            order_quantity,
            order_timestamp,
            status
        FROM orders 
        WHERE partition_date = '{partition_date}'
        """
        
        df = pd.read_sql(query, engine)
        
        context['task_instance'].xcom_push(
            key='extracted_orders', 
            value=df.to_dict('records')
        )
        
        return f"Extracted {len(df)} orders for {partition_date}"
        
    except Exception as e:
        raise Exception(f"Extraction failed: {str(e)}")


def transform_orders(**context) -> str:
    """
    Cleans customer information, validates order amounts/quantities, 
    and formats timestamps to ISO standard.
    """
    ti = context['task_instance']
    orders_data = ti.xcom_pull(task_ids='extract_incremental', key='extracted_orders')
    
    if not orders_data:
        raise ValueError("No data extracted for transformation")
    
    transformed_orders = []
    for order in orders_data:
        try:
            cleaned_order = {
                'order_id': order['order_id'],
                'customer_name': order['customer_name'].strip().title() if order.get('customer_name') else None,
                'customer_email': order['customer_email'].strip().lower() if order.get('customer_email') else None,
                'order_amount': float(order['order_amount']) if order.get('order_amount', 0) > 0 else 0,
                'order_quantity': int(order['order_quantity']) if order.get('order_quantity', 0) > 0 else 0,
                'order_timestamp': pd.to_datetime(order['order_timestamp']).isoformat() if order.get('order_timestamp') else None,
                'status': order.get('status')
            }
            
            if cleaned_order['order_amount'] > 0 and cleaned_order['order_quantity'] > 0:
                transformed_orders.append(cleaned_order)
                
        except (KeyError, ValueError, TypeError) as e:
            print(f"Skipping invalid order {order.get('order_id')}: {e}")
            continue
    
    ti.xcom_push(key='transformed_orders', value=transformed_orders)
    
    return f"Transformed {len(transformed_orders)} valid orders"


def load_orders_to_warehouse(**context) -> str:
    """
    Upserts transformed records into fact_orders table and updates related metrics.
    """
    ti = context['task_instance']
    transformed_orders = ti.xcom_pull(task_ids='transform', key='transformed_orders')
    
    if not transformed_orders:
        print("No transformed data to load")
        return "No data to load"
    
    try:
        conn = BaseHook.get_connection('database_conn')
        engine = sqlalchemy.create_engine(conn.get_uri())
        
        df = pd.DataFrame(transformed_orders)
        
        if df.empty:
            return "No valid orders to load"
        
        with engine.begin() as connection:
            df.to_sql('temp_fact_orders', connection, if_exists='replace', index=False)
            
            upsert_sql = """
            INSERT INTO fact_orders (
                order_id, customer_name, customer_email, order_amount, 
                order_quantity, order_timestamp, status
            )
            SELECT 
                order_id, customer_name, customer_email, order_amount, 
                order_quantity, order_timestamp, status
            FROM temp_fact_orders
            ON CONFLICT (order_id) DO UPDATE SET
                customer_name = EXCLUDED.customer_name,
                customer_email = EXCLUDED.customer_email,
                order_amount = EXCLUDED.order_amount,
                order_quantity = EXCLUDED.order_quantity,
                order_timestamp = EXCLUDED.order_timestamp,
                status = EXCLUDED.status,
                updated_at = CURRENT_TIMESTAMP;
            """
            
            connection.execute(sqlalchemy.text(upsert_sql))
            
            metrics_sql = """
            INSERT INTO daily_order_metrics (
                order_date, total_orders, total_amount, total_quantity
            )
            SELECT 
                DATE(order_timestamp) as order_date,
                COUNT(*) as total_orders,
                SUM(order_amount) as total_amount,
                SUM(order_quantity) as total_quantity
            FROM temp_fact_orders
            GROUP BY DATE(order_timestamp)
            ON CONFLICT (order_date) DO UPDATE SET
                total_orders = EXCLUDED.total_orders,
                total_amount = EXCLUDED.total_amount,
                total_quantity = EXCLUDED.total_quantity,
                updated_at = CURRENT_TIMESTAMP;
            """
            
            connection.execute(sqlalchemy.text(metrics_sql))
            connection.execute(sqlalchemy.text("DROP TABLE temp_fact_orders"))
        
        return f"Loaded {len(df)} orders to warehouse"
        
    except Exception as e:
        raise Exception(f"Load failed: {str(e)}")


with DAG(
    dag_id='daily_orders_etl',
    default_args=default_args,
    description='Daily ETL pipeline for incremental orders processing',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
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
        AND partition_name = 'p_{{ ds_nodash }}'
        LIMIT 1
        """,
        mode='reschedule',
        poke_interval=300,
        timeout=3600,
    )
    
    extract_incremental = PythonOperator(
        task_id='extract_incremental',
        python_callable=extract_incremental_orders,
    )
    
    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_orders,
    )
    
    load = PythonOperator(
        task_id='load',
        python_callable=load_orders_to_warehouse,
    )
    
    wait_partition >> extract_incremental >> transform >> load