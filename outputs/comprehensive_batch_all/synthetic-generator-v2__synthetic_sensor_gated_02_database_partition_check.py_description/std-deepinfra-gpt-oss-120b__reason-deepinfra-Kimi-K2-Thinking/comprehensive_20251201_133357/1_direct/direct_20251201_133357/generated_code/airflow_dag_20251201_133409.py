from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.sensors.sql import SqlSensor
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.hooks.sql import SqlHook

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}


def extract_incremental(**kwargs):
    """Extract new orders for the execution date partition."""
    ds = kwargs["ds"]  # execution date in YYYY-MM-DD format
    sql = f"""
        SELECT *
        FROM orders
        WHERE partition_date = DATE '{ds}'
    """
    hook = SqlHook(conn_id="database_conn")
    records = hook.get_records(sql)
    logging.info("Extracted %d rows for partition %s", len(records), ds)
    kwargs["ti"].xcom_push(key="raw_orders", value=records)


def transform(**kwargs):
    """Clean, validate and reformat the extracted orders."""
    raw_orders = kwargs["ti"].xcom_pull(key="raw_orders", task_ids="extract_incremental")
    transformed = []
    for row in raw_orders:
        # Placeholder for real transformation logic.
        transformed.append(row)
    logging.info("Transformed %d rows", len(transformed))
    kwargs["ti"].xcom_push(key="transformed_orders", value=transformed)


def load(**kwargs):
    """Upsert transformed records into the fact_orders table."""
    transformed = kwargs["ti"].xcom_pull(key="transformed_orders", task_ids="transform")
    hook = SqlHook(conn_id="database_conn")
    upsert_sql = """
        INSERT INTO fact_orders (col1, col2, col3)
        VALUES (%s, %s, %s)
        ON CONFLICT (primary_key) DO UPDATE SET
            col2 = EXCLUDED.col2,
            col3 = EXCLUDED.col3;
    """
    for record in transformed:
        hook.run(upsert_sql, parameters=record)
    logging.info("Loaded %d records into fact_orders", len(transformed))


with DAG(
    dag_id="daily_orders_etl",
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["etl", "orders"],
) as dag:
    wait_partition = SqlSensor(
        task_id="wait_partition",
        conn_id="database_conn",
        sql="""
            SELECT 1
            FROM information_schema.partitions
            WHERE table_name = 'orders'
              AND partition_name = '{{ ds }}'
            LIMIT 1
        """,
        mode="reschedule",
        poke_interval=300,  # 5 minutes
        timeout=3600,  # 1 hour
    )

    extract_incremental_task = PythonOperator(
        task_id="extract_incremental",
        python_callable=extract_incremental,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
        provide_context=True,
    )

    wait_partition >> extract_incremental_task >> transform_task >> load_task