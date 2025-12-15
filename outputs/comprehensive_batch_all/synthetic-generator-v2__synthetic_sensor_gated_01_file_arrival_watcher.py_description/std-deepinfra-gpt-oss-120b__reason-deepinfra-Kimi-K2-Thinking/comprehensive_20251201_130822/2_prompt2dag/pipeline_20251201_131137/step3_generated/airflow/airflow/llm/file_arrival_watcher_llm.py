# -*- coding: utf-8 -*-
"""
Generated Airflow DAG: file_arrival_watcher
Description: Monitors daily transaction file arrivals, validates schema, and loads data into PostgreSQL.
Generation timestamp: 2024-06-26T12:00:00Z
"""

import os
import time
import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# -------------------------------------------------------------------------
# Default arguments applied to all tasks
# -------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# -------------------------------------------------------------------------
# DAG definition
# -------------------------------------------------------------------------
with DAG(
    dag_id="file_arrival_watcher",
    description="Monitors daily transaction file arrivals, validates schema, and loads data into PostgreSQL.",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["monitoring", "validation", "postgresql"],
    max_active_runs=1,
    timezone="UTC",
) as dag:

    # -----------------------------------------------------------------
    # Helper constants (could be moved to Airflow Variables / Connections)
    # -----------------------------------------------------------------
    # Connection IDs defined in Airflow UI
    FS_CONN_ID = "local_filesystem"
    PG_CONN_ID = "postgresql_db"

    # Expected schema for the transaction file
    EXPECTED_COLUMNS = [
        "transaction_id",
        "account_id",
        "amount",
        "currency",
        "transaction_date",
    ]

    # -----------------------------------------------------------------
    # Task: Wait for Transaction File
    # -----------------------------------------------------------------
    @task(
        retries=2,
        retry_delay=timedelta(minutes=5),
        timeout=60 * 60,  # 1 hour max wait
    )
    def wait_for_file(**context):
        """
        Polls the local filesystem for the presence of the daily transaction file.
        The file name is expected to follow the pattern: transactions_YYYYMMDD.csv
        """
        execution_date = context["ds"]  # e.g., '2024-06-26'
        file_name = f"transactions_{execution_date.replace('-', '')}.csv"

        # Retrieve base path from Airflow Variable or fallback to a default
        base_path = Variable.get("transactions_input_path", default_var="/opt/airflow/incoming")
        file_path = os.path.join(base_path, file_name)

        logger = logging.getLogger("airflow.task.wait_for_file")
        logger.info("Looking for file %s", file_path)

        max_attempts = 12  # check every 5 minutes for up to 1 hour
        attempt = 0
        while attempt < max_attempts:
            if os.path.isfile(file_path):
                logger.info("File %s found.", file_path)
                # Push the path to XCom for downstream tasks
                return file_path
            logger.info("File not found. Waiting 5 minutes (attempt %d/%d).", attempt + 1, max_attempts)
            time.sleep(300)  # 5 minutes
            attempt += 1

        raise AirflowException(f"File {file_path} not found after waiting for 1 hour.")

    # -----------------------------------------------------------------
    # Task: Validate Transaction File Schema
    # -----------------------------------------------------------------
    @task(
        retries=2,
        retry_delay=timedelta(minutes=5),
    )
    def validate_schema(file_path: str):
        """
        Loads the CSV file into a pandas DataFrame and validates that the
        required columns are present and have appropriate data types.
        """
        logger = logging.getLogger("airflow.task.validate_schema")
        logger.info("Validating schema of file %s", file_path)

        try:
            df = pd.read_csv(file_path)
        except Exception as exc:
            raise AirflowException(f"Failed to read CSV file {file_path}: {exc}")

        missing_cols = set(EXPECTED_COLUMNS) - set(df.columns)
        if missing_cols:
            raise AirflowException(f"Missing required columns: {missing_cols}")

        # Simple type checks (could be expanded)
        if not pd.api.types.is_numeric_dtype(df["amount"]):
            raise AirflowException("Column 'amount' must be numeric.")

        if not pd.api.types.is_datetime64_any_dtype(pd.to_datetime(df["transaction_date"], errors="coerce")):
            raise AirflowException("Column 'transaction_date' must be a valid date.")

        logger.info("Schema validation passed.")
        # Return the validated DataFrame as JSON-serializable dict for downstream use
        return df.to_dict(orient="records")

    # -----------------------------------------------------------------
    # Task: Load Validated Transactions to PostgreSQL
    # -----------------------------------------------------------------
    @task(
        retries=2,
        retry_delay=timedelta(minutes=5),
    )
    def load_db(validated_records: list):
        """
        Inserts validated transaction records into the PostgreSQL database.
        """
        logger = logging.getLogger("airflow.task.load_db")
        logger.info("Loading %d records into PostgreSQL.", len(validated_records))

        if not validated_records:
            logger.warning("No records to load; exiting task.")
            return

        pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)

        insert_sql = """
            INSERT INTO transactions (
                transaction_id,
                account_id,
                amount,
                currency,
                transaction_date
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING;
        """

        # Prepare data tuples
        data_tuples = [
            (
                rec["transaction_id"],
                rec["account_id"],
                rec["amount"],
                rec["currency"],
                rec["transaction_date"],
            )
            for rec in validated_records
        ]

        try:
            pg_hook.run(insert_sql, parameters=data_tuples, autocommit=True)
            logger.info("Successfully loaded records into PostgreSQL.")
        except Exception as exc:
            raise AirflowException(f"Failed to load data into PostgreSQL: {exc}")

    # -----------------------------------------------------------------
    # Define task pipeline
    # -----------------------------------------------------------------
    file_path = wait_for_file()
    validated = validate_schema(file_path)
    load_db(validated)

    # Explicit dependencies (optional, as the chaining above already defines them)
    # wait_for_file >> validate_schema >> load_db

# End of DAG definition.