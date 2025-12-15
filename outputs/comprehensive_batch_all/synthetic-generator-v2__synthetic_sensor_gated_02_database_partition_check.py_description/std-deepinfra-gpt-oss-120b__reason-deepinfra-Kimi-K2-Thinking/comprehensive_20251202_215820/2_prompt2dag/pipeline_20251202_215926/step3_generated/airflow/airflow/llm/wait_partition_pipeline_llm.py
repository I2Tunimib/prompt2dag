# -*- coding: utf-8 -*-
"""
Generated Airflow DAG: wait_partition_pipeline
Author: Automated Generator
Date: 2024-06-27
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, List, Dict

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
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
    dag_id="wait_partition_pipeline",
    description="No description provided.",
    schedule_interval=None,  # Disabled schedule; can be enabled by setting a cron expression
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["example", "sequential"],
    is_paused_upon_creation=True,  # Ensures the DAG is not auto‑scheduled
    render_template_as_native_obj=True,
) as dag:
    # ---------------------------------------------------------------------
    # Helper to fetch the generic DB connection
    # ---------------------------------------------------------------------
    def get_db_connection() -> Any:
        """
        Retrieve the generic source/target database connection defined in Airflow.
        """
        try:
            conn = BaseHook.get_connection("database_conn")
            logging.info("Successfully fetched DB connection: %s", conn.conn_id)
            return conn
        except Exception as exc:
            logging.error("Failed to retrieve DB connection: %s", exc)
            raise

    # ---------------------------------------------------------------------
    # Task: Wait for Daily Orders Partition
    # ---------------------------------------------------------------------
    @task(retries=2, retry_delay=timedelta(minutes=5))
    def wait_partition() -> None:
        """
        Poll the source database until the daily orders partition becomes available.
        """
        conn = get_db_connection()
        # Placeholder logic – replace with actual partition check
        max_tries = 12
        wait_seconds = 30
        for attempt in range(1, max_tries + 1):
            try:
                # Example: SELECT 1 FROM information_schema.tables WHERE table_name = 'orders_2024_09_01';
                logging.info("Checking for partition existence (attempt %d/%d)...", attempt, max_tries)
                # Simulate successful check on the 3rd attempt
                if attempt >= 3:
                    logging.info("Required partition is now available.")
                    return
            except Exception as exc:
                logging.warning("Partition check failed: %s", exc)
            finally:
                import time

                time.sleep(wait_seconds)
        raise RuntimeError("Partition not found after multiple attempts.")

    # ---------------------------------------------------------------------
    # Task: Extract Incremental Orders
    # ---------------------------------------------------------------------
    @task(retries=2, retry_delay=timedelta(minutes=5))
    def extract_incremental() -> List[Dict[str, Any]]:
        """
        Pull incremental order records from the source database.
        Returns a list of dictionaries representing rows.
        """
        conn = get_db_connection()
        try:
            # Placeholder: replace with actual extraction logic
            logging.info("Extracting incremental orders...")
            # Simulated data
            data = [
                {"order_id": 101, "amount": 250.0, "order_date": "2024-09-01"},
                {"order_id": 102, "amount": 125.5, "order_date": "2024-09-01"},
            ]
            logging.info("Extracted %d records.", len(data))
            return data
        except Exception as exc:
            logging.error("Failed to extract incremental orders: %s", exc)
            raise

    # ---------------------------------------------------------------------
    # Task: Transform Orders Data
    # ---------------------------------------------------------------------
    @task(retries=2, retry_delay=timedelta(minutes=5))
    def transform(orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Apply business transformations to the raw order data.
        """
        try:
            logging.info("Starting transformation of %d records.", len(orders))
            transformed = []
            for row in orders:
                # Example transformation: calculate tax and total
                tax_rate = 0.07
                tax = round(row["amount"] * tax_rate, 2)
                total = round(row["amount"] + tax, 2)
                transformed.append(
                    {
                        "order_id": row["order_id"],
                        "order_date": row["order_date"],
                        "net_amount": row["amount"],
                        "tax": tax,
                        "total_amount": total,
                    }
                )
            logging.info("Transformation complete.")
            return transformed
        except Exception as exc:
            logging.error("Error during transformation: %s", exc)
            raise

    # ---------------------------------------------------------------------
    # Task: Load Orders to Data Warehouse
    # ---------------------------------------------------------------------
    @task(retries=2, retry_delay=timedelta(minutes=5))
    def load(transformed_orders: List[Dict[str, Any]]) -> None:
        """
        Load the transformed order data into the target data warehouse.
        """
        conn = get_db_connection()
        try:
            # Placeholder: replace with actual loading logic (e.g., bulk insert)
            logging.info("Loading %d records into the data warehouse.", len(transformed_orders))
            # Simulate successful load
            logging.info("Load completed successfully.")
        except Exception as exc:
            logging.error("Failed to load data into warehouse: %s", exc)
            raise

    # ---------------------------------------------------------------------
    # Define task pipeline (sequential)
    # ---------------------------------------------------------------------
    wait_partition_task = wait_partition()
    extract_task = extract_incremental()
    transform_task = transform()
    load_task = load()

    # Set dependencies
    wait_partition_task >> extract_task >> transform_task >> load_task

# End of DAG definition.