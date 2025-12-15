from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.hooks.sql import SqlHook


def extract_incremental(**context):
    """
    Extract new orders for the current date partition from the source database.
    Returns the fetched rows which are passed to downstream tasks via XCom.
    """
    hook = SqlHook(conn_id="database_conn")
    query = """
        SELECT *
        FROM orders
        WHERE partition_date = CURRENT_DATE
    """
    rows = hook.get_records(sql=query)
    return rows


def transform(**context):
    """
    Clean and validate extracted order records.
    Performs simple transformations and returns the processed data.
    """
    ti = context["ti"]
    orders = ti.xcom_pull(task_ids="extract_incremental")
    transformed = []

    for row in orders:
        # Assuming row is a tuple; convert to dict for readability
        # Adjust indices according to actual table schema
        order = {
            "order_id": row[0],
            "customer_name": row[1].strip() if isinstance(row[1], str) else row[1],
            "order_amount": float(row[2]) if row[2] is not None else 0.0,
            "quantity": int(row[3]) if row[3] is not None else 0,
            "order_timestamp": (
                row[4].isoformat()
                if hasattr(row[4], "isoformat")
                else str(row[4])
            ),
        }

        # Simple validation
        if order["order_amount"] <= 0 or order["quantity"] <= 0:
            continue  # Skip invalid records

        transformed.append(order)

    return transformed


def load(**context):
    """
    Upsert transformed order records into the target fact_orders table.
    """
    ti = context["ti"]
    records = ti.xcom_pull(task_ids="transform")
    if not records:
        return

    hook = SqlHook(conn_id="database_conn")
    upsert_sql = """
        INSERT INTO fact_orders (
            order_id,
            customer_name,
            order_amount,
            quantity,
            order_timestamp
        ) VALUES (
            %(order_id)s,
            %(customer_name)s,
            %(order_amount)s,
            %(quantity)s,
            %(order_timestamp)s
        )
        ON CONFLICT (order_id) DO UPDATE SET
            customer_name = EXCLUDED.customer_name,
            order_amount = EXCLUDED.order_amount,
            quantity = EXCLUDED.quantity,
            order_timestamp = EXCLUDED.order_timestamp;
    """

    for record in records:
        hook.run(sql=upsert_sql, parameters=record)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

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
        poke_interval=300,  # 5 minutes
        timeout=3600,  # 1 hour
        mode="reschedule",
    )

    extract_incremental_task = PythonOperator(
        task_id="extract_incremental",
        python_callable=extract_incremental,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    wait_partition >> extract_incremental_task >> transform_task >> load_task