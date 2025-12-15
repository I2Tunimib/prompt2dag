from datetime import datetime, timedelta

from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
import pandas as pd
import sqlalchemy


def validate_schema(file_path: str, **kwargs) -> None:
    """
    Validate the schema of the CSV file.

    Expected columns:
        - transaction_id (string)
        - customer_id (string)
        - amount (decimal)
        - transaction_date (date)

    Raises:
        ValueError: If columns or data types do not match expectations.
    """
    df = pd.read_csv(file_path)

    expected_columns = ["transaction_id", "customer_id", "amount", "transaction_date"]
    if list(df.columns) != expected_columns:
        raise ValueError(f"Unexpected columns. Expected {expected_columns}, got {list(df.columns)}")

    # Simple dtype checks
    if not pd.api.types.is_string_dtype(df["transaction_id"]):
        raise ValueError("transaction_id must be of string type")
    if not pd.api.types.is_string_dtype(df["customer_id"]):
        raise ValueError("customer_id must be of string type")
    if not pd.api.types.is_numeric_dtype(df["amount"]):
        raise ValueError("amount must be numeric (decimal) type")
    if not pd.api.types.is_datetime64_any_dtype(df["transaction_date"]):
        # Attempt to parse dates if not already datetime
        try:
            pd.to_datetime(df["transaction_date"])
        except Exception as exc:
            raise ValueError("transaction_date must be a date type") from exc


def load_db(file_path: str, **kwargs) -> None:
    """
    Load the validated CSV data into PostgreSQL.

    The target table is public.transactions.
    """
    # Adjust the connection string as needed for your environment
    engine = sqlalchemy.create_engine(
        "postgresql://postgres:postgres@localhost:5432/postgres"
    )
    df = pd.read_csv(file_path)

    # Ensure transaction_date is a datetime for proper insertion
    if not pd.api.types.is_datetime64_any_dtype(df["transaction_date"]):
        df["transaction_date"] = pd.to_datetime(df["transaction_date"])

    df.to_sql(
        name="transactions",
        con=engine,
        schema="public",
        if_exists="append",
        index=False,
        method="multi",
    )


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_transaction_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["example", "transactions"],
) as dag:
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/data/incoming/transactions_{{ ds_nodash }}.csv",
        poke_interval=30,
        timeout=24 * 60 * 60,  # 24 hours
        mode="poke",
    )

    validate_schema_task = PythonOperator(
        task_id="validate_schema",
        python_callable=validate_schema,
        op_kwargs={"file_path": "/data/incoming/transactions_{{ ds_nodash }}.csv"},
    )

    load_db_task = PythonOperator(
        task_id="load_db",
        python_callable=load_db,
        op_kwargs={"file_path": "/data/incoming/transactions_{{ ds_nodash }}.csv"},
    )

    wait_for_file >> validate_schema_task >> load_db_task