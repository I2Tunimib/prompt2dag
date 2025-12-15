# -*- coding: utf-8 -*-
"""
Generated Airflow DAG: wait_for_file_pipeline
Description: Sensor‑gated pipeline that monitors daily transaction file arrivals,
validates the file schema, and loads the data into a PostgreSQL table.
Generated on: 2024-06-28
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.models import BaseHook

# -------------------------------------------------------------------------
# Default arguments applied to all tasks
# -------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,                     # overridden per‑task where needed
    "retry_delay": timedelta(minutes=5),
}

# -------------------------------------------------------------------------
# DAG definition
# -------------------------------------------------------------------------
with DAG(
    dag_id="wait_for_file_pipeline",
    description="Sensor‑gated pipeline that monitors daily transaction file arrivals, validates the file schema, and loads the data into a PostgreSQL table.",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["example", "file", "postgres"],
    max_active_runs=1,
    render_template_as_native_obj=True,
) as dag:

    # ---------------------------------------------------------------------
    # Helper: retrieve base directory from the `local_filesystem` connection
    # ---------------------------------------------------------------------
    def _get_base_path() -> str:
        """
        Returns the base directory for incoming transaction files.
        The connection `local_filesystem` must contain an ``extra`` JSON with a ``path`` key.
        """
        conn = BaseHook.get_connection("local_filesystem")
        # Example extra: {"path": "/opt/airflow/data/incoming"}
        path = conn.extra_dejson.get("path")
        if not path:
            raise AirflowException(
                "Connection `local_filesystem` must define an extra JSON key `path`."
            )
        return path

    # ---------------------------------------------------------------------
    # Task: wait_for_file
    # ---------------------------------------------------------------------
    @task(
        task_id="wait_for_file",
        retries=0,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(hours=2),
    )
    def wait_for_file(ds: str = "{{ ds }}") -> str:
        """
        Checks for the presence of the daily transaction file.
        Expected filename pattern: ``transactions_<YYYY-MM-DD>.csv``.
        Returns the absolute file path if found.
        """
        base_path = _get_base_path()
        filename = f"transactions_{ds}.csv"
        file_path = os.path.join(base_path, filename)

        if not os.path.isfile(file_path):
            raise AirflowException(f"Transaction file not found: {file_path}")

        return file_path

    # ---------------------------------------------------------------------
    # Task: validate_schema
    # ---------------------------------------------------------------------
    @task(
        task_id="validate_schema",
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=1),
    )
    def validate_schema(file_path: str) -> str:
        """
        Validates that the CSV file conforms to the expected schema.
        Expected columns (order is not important):
            - transaction_id
            - transaction_date
            - amount
            - currency
            - status
        Returns the same file_path for downstream consumption.
        """
        expected_columns = {
            "transaction_id",
            "transaction_date",
            "amount",
            "currency",
            "status",
        }

        try:
            df = pd.read_csv(file_path, nrows=5)  # read a small sample for speed
        except Exception as exc:
            raise AirflowException(f"Failed to read CSV file {file_path}: {exc}")

        missing = expected_columns - set(df.columns)
        extra = set(df.columns) - expected_columns

        if missing:
            raise AirflowException(
                f"Schema validation error – missing columns: {sorted(missing)}"
            )
        if extra:
            raise AirflowException(
                f"Schema validation error – unexpected columns: {sorted(extra)}"
            )

        return file_path

    # ---------------------------------------------------------------------
    # Task: load_db
    # ---------------------------------------------------------------------
    @task(
        task_id="load_db",
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=2),
    )
    def load_db(file_path: str) -> None:
        """
        Loads the validated transaction CSV into the PostgreSQL table ``public.transactions``.
        The table must exist with a compatible schema.
        """
        # Load full CSV into a DataFrame
        try:
            df = pd.read_csv(file_path)
        except Exception as exc:
            raise AirflowException(f"Failed to load CSV for DB insertion: {exc}")

        # Convert DataFrame to list of tuples for bulk insert
        records = list(df.itertuples(index=False, name=None))

        # Define insert statement – adjust column order to match the table definition
        insert_sql = """
            INSERT INTO public.transactions
                (transaction_id, transaction_date, amount, currency, status)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO UPDATE
                SET transaction_date = EXCLUDED.transaction_date,
                    amount = EXCLUDED.amount,
                    currency = EXCLUDED.currency,
                    status = EXCLUDED.status;
        """

        hook = PostgresHook(postgres_conn_id="postgres_local")
        try:
            hook.run(insert_sql, parameters=records, autocommit=True)
        except Exception as exc:
            raise AirflowException(f"Failed to insert records into PostgreSQL: {exc}")

    # ---------------------------------------------------------------------
    # Define task pipeline
    # ---------------------------------------------------------------------
    file_path = wait_for_file()
    validated_path = validate_schema(file_path)
    load_db(validated_path)

# End of DAG definition.