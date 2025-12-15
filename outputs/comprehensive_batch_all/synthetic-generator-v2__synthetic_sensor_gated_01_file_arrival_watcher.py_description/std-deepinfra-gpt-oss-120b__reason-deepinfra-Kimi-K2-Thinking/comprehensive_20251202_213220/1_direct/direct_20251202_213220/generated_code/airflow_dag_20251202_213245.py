"""Daily transaction ingestion DAG."""

from datetime import datetime, timedelta
import logging
import os

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

EXPECTED_COLUMNS = ["transaction_id", "customer_id", "amount", "transaction_date"]


def validate_schema(**context):
    """Validate CSV schema for the transaction file."""
    file_path = context["templates_dict"]["file_path"]
    logging.info("Validating schema of file %s", file_path)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        header = f.readline().strip().split(",")

    if header != EXPECTED_COLUMNS:
        raise ValueError(
            f"Invalid columns. Expected {EXPECTED_COLUMNS}, got {header}"
        )
    logging.info("Schema validation passed for %s", file_path)


def load_to_postgres(**context):
    """Load validated CSV file into PostgreSQL."""
    file_path = context["templates_dict"]["file_path"]
    logging.info("Loading file %s into PostgreSQL", file_path)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    hook = PostgresHook(postgres_conn_id="postgres_default")
    copy_sql = f"""
        COPY public.transactions ({', '.join(EXPECTED_COLUMNS)})
        FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '"'
    """
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            with open(file_path, "r", encoding="utf-8") as f:
                cur.copy_expert(sql=copy_sql, file=f)
        conn.commit()
    finally:
        conn.close()
    logging.info("Loaded %s into public.transactions", file_path)


with DAG(
    dag_id="daily_transaction_ingestion",
    default_args=DEFAULT_ARGS,
    description="Ingest daily transaction CSV files into PostgreSQL.",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "postgres"],
) as dag:
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/data/incoming/transactions_{{ ds_nodash }}.csv",
        poke_interval=30,
        timeout=24 * 60 * 60,
        mode="poke",
    )

    validate = PythonOperator(
        task_id="validate_schema",
        python_callable=validate_schema,
        provide_context=True,
        templates_dict={
            "file_path": "/data/incoming/transactions_{{ ds_nodash }}.csv"
        },
    )

    load = PythonOperator(
        task_id="load_db",
        python_callable=load_to_postgres,
        provide_context=True,
        templates_dict={
            "file_path": "/data/incoming/transactions_{{ ds_nodash }}.csv"
        },
    )

    wait_for_file >> validate >> load