# -*- coding: utf-8 -*-
"""
Generated Airflow DAG for fraud_detection_triage
Author: Automated DAG Generator
Date: 2024-06-28
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# ----------------------------------------------------------------------
# Default arguments applied to all tasks
# ----------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# ----------------------------------------------------------------------
# DAG definition
# ----------------------------------------------------------------------
with DAG(
    dag_id="fraud_detection_triage",
    description="No description provided.",
    schedule=None,  # Disabled; enable by setting a cron expression like '@daily'
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["fraud", "triage", "fanout_fanin"],
    is_paused_upon_creation=True,  # Ensures the DAG starts in a paused state
    render_template_as_native_obj=True,
) as dag:

    # ------------------------------------------------------------------
    # Helper functions
    # ------------------------------------------------------------------
    def _get_connection(conn_id: str):
        """Retrieve an Airflow connection by its ID."""
        try:
            return BaseHook.get_connection(conn_id)
        except Exception as exc:
            logging.error("Failed to retrieve connection %s: %s", conn_id, exc)
            raise

    # ------------------------------------------------------------------
    # Task: Analyze Transactions
    # ------------------------------------------------------------------
    @task(retries=2, retry_delay=timedelta(minutes=5))
    def analyze_transactions() -> List[Dict[str, Any]]:
        """
        Load daily transaction CSV files from the ``transaction_csv_files`` FS connection,
        parse them, and return a list of transaction dictionaries.
        """
        conn = _get_connection("transaction_csv_files")
        # Assuming the connection provides a base path; adjust as needed.
        base_path = conn.extra_dejson.get("base_path", "/data/transactions")
        logging.info("Reading transaction CSV files from %s", base_path)

        # Placeholder: In a real implementation, you'd list files and read them.
        # Here we return a static example list.
        transactions = [
            {"transaction_id": "tx001", "amount": 120.0, "currency": "USD"},
            {"transaction_id": "tx002", "amount": 5000.0, "currency": "EUR"},
        ]
        logging.info("Loaded %d transactions", len(transactions))
        return transactions

    # ------------------------------------------------------------------
    # Task: Route Transaction
    # ------------------------------------------------------------------
    @task(retries=2, retry_delay=timedelta(minutes=5))
    def route_transaction(
        transactions: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Split transactions into two buckets:
        - ``auto_approve``: low‑risk transactions that can be processed automatically.
        - ``manual_review``: high‑risk transactions that require human review.
        """
        auto_approve = []
        manual_review = []

        for txn in transactions:
            # Simple rule: amounts > 1000 require manual review.
            if txn["amount"] > 1000:
                manual_review.append(txn)
            else:
                auto_approve.append(txn)

        logging.info(
            "Routing complete: %d auto‑approve, %d manual‑review",
            len(auto_approve),
            len(manual_review),
        )
        return {"auto_approve": auto_approve, "manual_review": manual_review}

    # ------------------------------------------------------------------
    # Task: Process Auto Approval
    # ------------------------------------------------------------------
    @task(retries=2, retry_delay=timedelta(minutes=5))
    def process_auto_approve(
        auto_transactions: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Call the ``payment_processing_api`` HTTP endpoint for each auto‑approved transaction.
        Returns a list of responses.
        """
        if not auto_transactions:
            logging.info("No auto‑approved transactions to process.")
            return []

        conn = _get_connection("payment_processing_api")
        endpoint = conn.host.rstrip("/") + "/process"

        responses = []
        for txn in auto_transactions:
            payload = json.dumps(txn)
            logging.info("Posting transaction %s to %s", txn["transaction_id"], endpoint)
            # Placeholder for actual HTTP request; replace with requests or HttpHook.
            response = {"transaction_id": txn["transaction_id"], "status": "processed"}
            responses.append(response)

        logging.info("Processed %d auto‑approved transactions", len(responses))
        return responses

    # ------------------------------------------------------------------
    # Task: Process Manual Review
    # ------------------------------------------------------------------
    @task(retries=2, retry_delay=timedelta(minutes=5))
    def process_manual_review(
        manual_transactions: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Publish each manual‑review transaction to the ``manual_review_queue`` RabbitMQ queue.
        Returns a list of acknowledgements.
        """
        if not manual_transactions:
            logging.info("No manual‑review transactions to enqueue.")
            return []

        conn = _get_connection("manual_review_queue")
        queue_name = conn.extra_dejson.get("queue", "manual_review")

        acknowledgements = []
        for txn in manual_transactions:
            message = json.dumps(txn)
            logging.info(
                "Publishing transaction %s to queue %s", txn["transaction_id"], queue_name
            )
            # Placeholder for actual publish; replace with pika or RabbitMQHook.
            ack = {"transaction_id": txn["transaction_id"], "queued": True}
            acknowledgements.append(ack)

        logging.info(
            "Enqueued %d manual‑review transactions", len(acknowledgements)
        )
        return acknowledgements

    # ------------------------------------------------------------------
    # Task: Send Notification
    # ------------------------------------------------------------------
    @task(
        retries=2,
        retry_delay=timedelta(minutes=5),
        trigger_rule=TriggerRule.ALL_DONE,
    )
    def send_notification(
        auto_responses: List[Dict[str, Any]],
        manual_acks: List[Dict[str, Any]],
    ) -> None:
        """
        Notify the fraud detection notification service about the processing outcome.
        This task runs regardless of the success/failure of upstream tasks (ALL_DONE).
        """
        conn = _get_connection("notification_service")
        endpoint = conn.host.rstrip("/") + "/notify"

        summary = {
            "auto_processed": len(auto_responses),
            "manual_queued": len(manual_acks),
        }
        payload = json.dumps(summary)
        logging.info("Sending notification to %s with payload %s", endpoint, payload)

        # Placeholder for actual HTTP request; replace with requests or HttpHook.
        logging.info("Notification sent successfully.")

    # ------------------------------------------------------------------
    # Define task pipeline
    # ------------------------------------------------------------------
    transactions = analyze_transactions()
    routed = route_transaction(transactions)

    auto_processed = process_auto_approve(routed["auto_approve"])
    manual_queued = process_manual_review(routed["manual_review"])

    send_notification(auto_processed, manual_queued)

    # ------------------------------------------------------------------
    # Set explicit dependencies (fan‑out / fan‑in pattern)
    # ------------------------------------------------------------------
    # route_transaction depends on analyze_transactions
    # process_auto_approve & process_manual_review depend on route_transaction
    # send_notification depends on both processing branches
    # Dependencies are already expressed via upstream/downstream arguments above.
    # The >> operator can be used for clarity:
    analyze_transactions() >> route_transaction()
    route_transaction() >> [process_auto_approve(), process_manual_review()]
    [process_auto_approve(), process_manual_review()] >> send_notification()