from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.hooks.sql import SqlHook
import pandas as pd

default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_orders_etl',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=['etl', 'orders', 'daily'],
    description='Daily ETL pipeline for incremental orders data processing',
) as dag:

    wait_partition = SqlSensor(
        task_id='wait_partition',
        conn_id='database_conn',
        sql="""
            SELECT 1
            FROM INFORMATION_SCHEMA.PARTITIONS
            WHERE TABLE_NAME = 'orders'
              AND PARTITION_NAME = '{{ ds_nodash }}'
        """,
        poke_interval=300,
        timeout=3600,
        mode='reschedule',
    )

    def extract_incremental_orders(**context):
        """Extract new orders from the daily partition."""
        ds = context['ds']
        hook = SqlHook(sql_conn_id='database_conn')
        sql = f"""
            SELECT order_id, customer_id, order_amount, quantity, 
                   customer_name, order_timestamp
            FROM orders
            WHERE partition_date = '{ds}'
        """
        df = hook.get_pandas_df(sql)
        return df.to_dict('records')

    extract_incremental = PythonOperator(
        task_id='extract_incremental',
        python_callable=extract_incremental_orders,
    )

    def transform_orders(**context):
        """Clean and validate orders data."""
        ti = context['ti']
        records = ti.xcom_pull(task_ids='extract_incremental')
        
        if not records:
            return []
        
        df = pd.DataFrame(records)
        
        df['customer_name'] = df['customer_name'].str.strip().str.title()
        df = df[(df['order_amount'] > 0) & (df['quantity'] > 0)]
        df['order_timestamp'] = pd.to_datetime(df['order_timestamp']).dt.isoformat()
        
        return df.to_dict('records')

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_orders,
    )

    def load_orders_to_warehouse(**context):
        """Upsert transformed orders into fact_orders table."""
        ti = context['ti']
        records = ti.xcom_pull(task_ids='transform')
        
        if not records:
            return
        
        hook = SqlHook(sql_conn_id='warehouse_conn')
        df = pd.DataFrame(records)
        
        hook.insert_rows(
            table='fact_orders',
            rows=df.values.tolist(),
            target_fields=df.columns.tolist(),
            commit_every=1000,
            replace=True,
        )
        
        ds = context['ds']
        hook.run(f"CALL update_order_metrics('{ds}')")

    load = PythonOperator(
        task_id='load',
        python_callable=load_orders_to_warehouse,
    )

    wait_partition >> extract_incremental >> transform >> load