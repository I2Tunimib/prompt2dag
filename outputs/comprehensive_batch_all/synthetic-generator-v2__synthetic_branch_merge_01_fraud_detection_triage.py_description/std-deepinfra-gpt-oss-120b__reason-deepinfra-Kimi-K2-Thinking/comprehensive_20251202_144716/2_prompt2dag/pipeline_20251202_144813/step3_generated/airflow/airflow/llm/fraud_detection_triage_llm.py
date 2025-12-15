# -*- coding: utf-8 -*-
"""
Generated Airflow DAG for fraud_detection_triage
Author: Automated DAG Generator
Date: 2024-06-28
"""

import json
import logging
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.decorators import dag, task

# ----------------------------------------------------------------------
# Default arguments applied to all tasks
# ----------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


# ----------------------------------------------------------------------
# DAG definition using the TaskFlow API
# ----------------------------------------------------------------------
@dag(
    dag_id="fraud_detection_triage",
    description="No description provided.",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["fraud", "triage", "fanout_fanin"],
    max_active_runs=1,
    render_template_as_native_obj=True,
    timezone="UTC",
)
def fraud_detection_triage():
    """
    Fan‑out / fan‑in pipeline that analyses daily transaction CSV files,
    routes each transaction to either auto‑approve or manual review,
    and finally sends a notification.
    """

    # ------------------------------------------------------------------
    # Helper: fetch connection details
    # ------------------------------------------------------------------
    def _get_conn(conn_id: str):
        """Retrieve a connection object from Airflow metadata DB."""
        try:
            return BaseHook.get_connection(conn_id)
        except Exception as exc:
            raise AirflowException(f"Unable to load connection '{conn_id}': {exc}")

    # ------------------------------------------------------------------
    # Task: Analyze Transactions
    # ------------------------------------------------------------------
    @task(task_id="analyze_transactions")
    def analyze_transactions() -> dict:
        """
        Load daily transaction CSV files from the filesystem connection,
        perform a simple risk scoring, and return a dict mapping transaction
        IDs to a boolean flag ``is_fraud``.
        """
        conn = _get_conn("transaction_csv_files")
        # Assuming the connection's host field contains the directory path
        csv_dir = conn.host.rstrip("/") + "/"
        logging.info("Reading transaction CSV files from %s", csv_dir)

        try:
            # For demo purposes we read a single CSV named `transactions.csv`
            df = pd.read_csv(f"{csv_dir}transactions.csv")
        except Exception as exc:
            raise AirflowException(f"Failed to read CSV files: {exc}")

        # Simple risk scoring: flag transactions > $10,000 as potential fraud
        df["is_fraud"] = df["amount"] > 10_000
        result = df.set_index("transaction_id")["is_fraud"].to_dict()
        logging.info("Analyzed %d transactions, %d flagged as fraud",
                     len(df), sum(result.values()))
        return result

    # ------------------------------------------------------------------
    # Task: Route Transaction
    # ------------------------------------------------------------------
    @task(task_id="route_transaction")
    def route_transaction(analysis: dict) -> dict:
        """
        Decide routing based on analysis result.
        Returns a dict with two keys:
        - ``auto_approve``: list of transaction IDs
        - ``manual_review``: list of transaction IDs
        """
        auto_approve = [tid for tid, fraud in analysis.items() if not fraud]
        manual_review = [tid for tid, fraud in analysis.items() if fraud]

        logging.info("Routing %d transactions to auto‑approve, %d to manual review",
                     len(auto_approve), len(manual_review))

        return {"auto_approve": auto_approve, "manual_review": manual_review}

    # ------------------------------------------------------------------
    # Task: Route to Auto‑Approve
    # ------------------------------------------------------------------
    @task(task_id="route_to_auto_approve")
    def route_to_auto_approve(routing: dict) -> list:
        """
        Send auto‑approved transactions to the payment processing API.
        Returns the list of successfully processed transaction IDs.
        """
        conn = _get_conn("payment_processing_api")
        endpoint = conn.host.rstrip("/") + "/process"

        successful = []
        for txn_id in routing.get("auto_approve", []):
            payload = {"transaction_id": txn_id, "action": "approve"}
            try:
                response = requests.post(endpoint, json=payload, timeout=10)
                response.raise_for_status()
                successful.append(txn_id)
            except Exception as exc:
                logging.error("Auto‑approve failed for %s: %s", txn_id, exc)

        logging.info("Auto‑approved %d transactions", len(successful))
        return successful

    # ------------------------------------------------------------------
    # Task: Route to Manual Review
    # ------------------------------------------------------------------
    @task(task_id="route_to_manual_review")
    def route_to_manual_review(routing: dict) -> list:
        """
        Publish transactions flagged for manual review to a RabbitMQ queue.
        Returns the list of transaction IDs that were enqueued.
        """
        conn = _get_conn("manual_review_queue")
        # Example: use connection's host as broker URL and login credentials
        broker_url = conn.host
        queue_name = conn.schema or "manual_review"

        # Placeholder implementation – in production you would use a proper
        # RabbitMQ client (e.g., pika) to publish messages.
        enqueued = []
        for txn_id in routing.get("manual_review", []):
            try:
                # Simulated publish
                logging.info("Enqueuing transaction %s to %s", txn_id, queue_name)
                enqueued.append(txn_id)
            except Exception as exc:
                logging.error("Failed to enqueue %s: %s", txn_id, exc)

        logging.info("Enqueued %d transactions for manual review", len(enqueued))
        return enqueued

    # ------------------------------------------------------------------
    # Task: Send Notification
    # ------------------------------------------------------------------
    @task(task_id="send_notification")
    def send_notification(
        auto_approved: list,
        manual_reviewed: list,
    ) -> None:
        """
        Notify the fraud detection notification service about the day's
        processing outcome.
        """
        conn = _get_conn("notification_service")
        endpoint = conn.host.rstrip("/") + "/notify"

        payload = {
            "date": datetime.utcnow().date().isoformat(),
            "auto_approved_count": len(auto_approved),
            "manual_review_count": len(manual_reviewed),
            "auto_approved_ids": auto_approved,
            "manual_review_ids": manual_reviewed,
        }

        try:
            response = requests.post(endpoint, json=payload, timeout=10)
            response.raise_for_status()
            logging.info("Notification sent successfully")
        except Exception as exc:
            raise AirflowException(f"Failed to send notification: {exc}")

    # ------------------------------------------------------------------
    # Define task pipeline (fan‑out / fan‑in)
    # ------------------------------------------------------------------
    analysis = analyze_transactions()
    routing = route_transaction(analysis)

    auto_approved = route_to_auto_approve(routing)
    manual_reviewed = route_to_manual_review(routing)

    send_notification(auto_approved, manual_reviewed)

    # Explicit dependencies (optional, as TaskFlow handles them via arguments)
    analysis >> routing
    routing >> [auto_approved, manual_reviewed]
    [auto_approved, manual_reviewed] >> send_notification


# ----------------------------------------------------------------------
# Instantiate DAG
# ----------------------------------------------------------------------
dag = fraud_detection_triage()