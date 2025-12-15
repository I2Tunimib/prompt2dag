# -*- coding: utf-8 -*-
"""
Generated Airflow DAG: wait_partition_pipeline
Description: Database Partition Check ETL
Generation Time: 2024-06-13T12:00:00Z
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule

# ----------------------------------------------------------------------
# Default arguments applied to all tasks unless overridden
# ----------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,  # individual tasks define their own retry count
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# ----------------------------------------------------------------------
# Failure callback for better observability
# ----------------------------------------------------------------------
def _task_failure_callback(context):
    """Log task failure details."""
    task_instance = context.get("task_instance")
    dag_id = context.get("dag").dag_id
    logging.error(
        "Task failed: %s.%s (execution_date=%s)",
        dag_id,
        task_instance.task_id,
        context.get("execution_date"),
    )

# ----------------------------------------------------------------------
# DAG definition
# ----------------------------------------------------------------------
with DAG(
    dag_id="wait_partition_pipeline",
    description="Database Partition Check ETL",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["etl", "partition"],
    is_paused_upon_creation=True,  # pipeline disabled by default
    max_active_runs=1,
    on_failure_callback=_task_failure_callback,
) as dag:

    # ------------------------------------------------------------------
    # Task: Wait for Daily Partition
    # ------------------------------------------------------------------
    wait_partition = SQLExecuteQueryOperator(
        task_id="wait_partition",
        sql="""
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = 'orders_{{ ds_nodash }}';
        """,
        conn_id="database_conn",
        retries=2,
        retry_delay=timedelta(minutes=5),
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
    )

    # ------------------------------------------------------------------
    # Task: Extract Incremental Orders
    # ------------------------------------------------------------------
    @task(retries=2, retry_delay=timedelta(minutes=5))
    def extract_incremental() -> list[dict]:
        """
        Pull incremental order records from the source database.
        Returns a list of dictionaries representing rows.
        """
        # Placeholder implementation – replace with real extraction logic.
        logging.info("Extracting incremental orders...")
        # Example dummy data
        return [
            {"order_id": 1, "amount": 100.0, "created_at": "2024-06-12"},
            {"order_id": 2, "amount": 250.5, "created_at": "2024-06-12"},
        ]

    # ------------------------------------------------------------------
    # Task: Transform Orders Data
    # ------------------------------------------------------------------
    @task(retries=2, retry_delay=timedelta(minutes=5))
    def transform_orders(raw_orders: list[dict]) -> list[dict]:
        """
        Apply business transformations to raw order data.
        """
        logging.info("Transforming %d orders...", len(raw_orders))
        transformed = []
        for order in raw_orders:
            # Example transformation: add a tax field
            tax = round(order["amount"] * 0.07, 2)
            order_copy = order.copy()
            order_copy["tax"] = tax
            transformed.append(order_copy)
        return transformed

    # ------------------------------------------------------------------
    # Task: Load Orders to Warehouse
    # ------------------------------------------------------------------
    @task(retries=2, retry_delay=timedelta(minutes=5))
    def load_orders(orders: list[dict]) -> None:
        """
        Load transformed orders into the data warehouse.
        """
        logging.info("Loading %d orders into the warehouse...", len(orders))
        # Placeholder: replace with actual loading logic (e.g., using a DB hook)
        for order in orders:
            logging.debug("Loaded order: %s", order)

    # ------------------------------------------------------------------
    # Define task pipeline
    # ------------------------------------------------------------------
    extracted = extract_incremental()
    transformed = transform_orders(extracted)
    loaded = load_orders(transformed)

    # Set explicit dependencies
    wait_partition >> extracted >> transformed >> loaded

# End of DAG definition.