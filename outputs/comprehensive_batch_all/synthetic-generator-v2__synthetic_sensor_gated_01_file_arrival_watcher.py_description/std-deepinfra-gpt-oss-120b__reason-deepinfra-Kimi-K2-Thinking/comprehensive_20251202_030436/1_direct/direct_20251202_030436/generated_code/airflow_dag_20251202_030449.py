import logging
import os
import csv
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

# File pattern template
FILE_PATH_TEMPLATE = "/data/incoming/transactions_{{ ds_nodash }}.csv"


def validate_schema(file_path: str, **context):
    """
    Validate the CSV file schema.

    Checks that required columns exist and that each row conforms to expected data types:
    - transaction_id: string
    - customer_id: string
    - amount: decimal
    - transaction_date: date (YYYY-MM-DD)
    """
    required_columns = ["transaction_id", "customer_id", "amount", "transaction_date"]
    logger = logging.getLogger("validate_schema")
    logger.info("Validating schema for file %s", file_path)

    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    with open(file_path, newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        header = reader.fieldnames
        if header is None:
            raise ValueError("CSV file has no header row.")
        missing = set(required_columns) - set(header)
        if missing:
            raise ValueError(f"Missing required columns: {missing}")

        for line_number, row in enumerate(reader, start=2):
            # transaction_id and customer_id are strings – no conversion needed
            # Validate amount as decimal
            try:
                float(row["amount"])
            except ValueError:
                raise ValueError(
                    f"Invalid decimal value in 'amount' at line {line_number}: {row['amount']}"
                )
            # Validate transaction_date as YYYY-MM-DD
            try:
                datetime.strptime(row["transaction_date"], "%Y-%m-%d")
            except ValueError:
                raise ValueError(
                    f"Invalid date format in 'transaction_date' at line {line_number}: {row['transaction_date']}"
                )
    logger.info("Schema validation passed for %s", file_path)


def load_db(file_path: str, **context):
    """
    Load the validated CSV data into PostgreSQL table public.transactions.
    """
    logger = logging.getLogger("load_db")
    logger.info("Loading data from %s into PostgreSQL", file_path)

    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    conn = psycopg2.connect(host="localhost", port=5432, dbname="postgres")
    try:
        with conn.cursor() as cur:
            with open(file_path, "r") as f:
                # Skip header line for COPY
                next(f)
                copy_sql = """
                    COPY public.transactions (transaction_id, customer_id, amount, transaction_date)
                    FROM STDIN WITH CSV
                """
                cur.copy_expert(sql=copy_sql, file=f)
        conn.commit()
        logger.info("Data loaded successfully into public.transactions")
    finally:
        conn.close()


with DAG(
    dag_id="daily_transaction_pipeline",
    default_args=default_args,
    description="Sensor‑gated pipeline that validates and loads daily transaction CSV files.",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "transactions"],
) as dag:
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=FILE_PATH_TEMPLATE,
        poke_interval=30,
        timeout=24 * 60 * 60,  # 24 hours
        mode="poke",
    )

    validate_schema_task = PythonOperator(
        task_id="validate_schema",
        python_callable=validate_schema,
        op_kwargs={"file_path": FILE_PATH_TEMPLATE},
    )

    load_db_task = PythonOperator(
        task_id="load_db",
        python_callable=load_db,
        op_kwargs={"file_path": FILE_PATH_TEMPLATE},
    )

    wait_for_file >> validate_schema_task >> load_db_task