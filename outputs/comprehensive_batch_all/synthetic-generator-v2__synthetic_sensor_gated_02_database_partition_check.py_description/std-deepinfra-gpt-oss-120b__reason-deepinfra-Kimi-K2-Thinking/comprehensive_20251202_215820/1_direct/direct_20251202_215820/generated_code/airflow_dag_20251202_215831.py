from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.sql import SqlSensor
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1, tzinfo=pendulum.UTC),
}


def extract_incremental(**kwargs):
    """
    Extract new orders for the current date partition from the source database.
    """
    hook = PostgresHook(postgres_conn_id="database_conn")
    sql = """
        SELECT *
        FROM orders
        WHERE partition_date = CURRENT_DATE
    """
    records = hook.get_records(sql)
    kwargs["ti"].xcom_push(key="raw_orders", value=records)


def transform(**kwargs):
    """
    Clean and validate extracted orders, then format timestamps to ISO.
    """
    ti = kwargs["ti"]
    raw_orders = ti.xcom_pull(key="raw_orders", task_ids="extract_incremental")
    transformed = []

    for row in raw_orders:
        # Placeholder transformation logic; replace with real cleaning/validation.
        transformed.append(row)

    ti.xcom_push(key="transformed_orders", value=transformed)


def load(**kwargs):
    """
    Upsert transformed records into the fact_orders table in the data warehouse.
    """
    ti = kwargs["ti"]
    transformed = ti.xcom_pull(key="transformed_orders", task_ids="transform")
    hook = PostgresHook(postgres_conn_id="database_conn")

    upsert_sql = """
        INSERT INTO fact_orders (col1, col2, col3)
        VALUES (%s, %s, %s)
        ON CONFLICT (col1) DO UPDATE
        SET col2 = EXCLUDED.col2,
            col3 = EXCLUDED.col3
    """

    for record in transformed:
        # Adjust the number of placeholders to match the record structure.
        hook.run(upsert_sql, parameters=record)


with DAG(
    dag_id="daily_incremental_orders_etl",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "orders"],
) as dag:
    wait_partition = SqlSensor(
        task_id="wait_partition",
        conn_id="database_conn",
        sql="""
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'orders'
              AND EXISTS (
                  SELECT 1
                  FROM orders
                  WHERE partition_date = CURRENT_DATE
              )
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