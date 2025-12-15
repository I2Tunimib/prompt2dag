# Generated Airflow DAG: load_customer_segment_csv_pipeline
# Generation timestamp: 2024-06-28 12:00:00 UTC
# --------------------------------------------------------------
# This DAG loads a customer segment CSV file and fans out three
# parallel campaigns: Email, Push Notification, and SMS.
# --------------------------------------------------------------

from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.utils.dates import days_ago

# ----------------------------------------------------------------------
# Default arguments applied to all tasks unless overridden
# ----------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ----------------------------------------------------------------------
# DAG definition
# ----------------------------------------------------------------------
with DAG(
    dag_id="load_customer_segment_csv_pipeline",
    description="No description provided.",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["load_customer_segment_csv_pipeline"],
    is_paused_upon_creation=True,  # Disabled by default
    max_active_runs=1,
) as dag:

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def load_customer_segment_csv() -> list:
        """
        Load customer segment data from a local CSV file.

        Returns:
            List[dict]: Records ready to be consumed by downstream tasks.
        """
        try:
            # Path can be externalised via Variable/Connection if needed
            csv_path = "/opt/airflow/data/customer_segment.csv"
            df = pd.read_csv(csv_path)
            # Convert DataFrame to JSON‑serialisable list of dicts
            return df.to_dict(orient="records")
        except Exception as exc:
            raise AirflowException(f"Failed to load CSV file: {exc}")

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def send_email_campaign(customers: list) -> None:
        """
        Send an email campaign using the ``email_service_api`` connection.

        Args:
            customers (list): List of customer records.
        """
        try:
            conn = BaseHook.get_connection("email_service_api")
            endpoint = conn.host.rstrip("/") + "/send"
            headers = {"Authorization": f"Bearer {conn.password}"}
            payload = {
                "recipients": customers,
                "template_id": "customer_segment",
            }
            response = requests.post(
                endpoint, json=payload, headers=headers, timeout=30
            )
            response.raise_for_status()
        except Exception as exc:
            raise AirflowException(f"Email campaign failed: {exc}")

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def send_push_notification(customers: list) -> None:
        """
        Send a push notification campaign using the
        ``push_notification_service_api`` connection.

        Args:
            customers (list): List of customer records.
        """
        try:
            conn = BaseHook.get_connection("push_notification_service_api")
            endpoint = conn.host.rstrip("/") + "/push"
            headers = {"Authorization": f"Bearer {conn.password}"}
            payload = {
                "users": customers,
                "message": "Check out our new offers!",
            }
            response = requests.post(
                endpoint, json=payload, headers=headers, timeout=30
            )
            response.raise_for_status()
        except Exception as exc:
            raise AirflowException(f"Push notification failed: {exc}")

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def send_sms_campaign(customers: list) -> None:
        """
        Send an SMS campaign using the ``sms_gateway_api`` connection.

        Args:
            customers (list): List of customer records.
        """
        try:
            conn = BaseHook.get_connection("sms_gateway_api")
            endpoint = conn.host.rstrip("/") + "/sms"
            headers = {"Authorization": f"Bearer {conn.password}"}
            # Assume each record has a ``phone`` field
            phone_numbers = [c["phone"] for c in customers if "phone" in c]
            payload = {
                "recipients": phone_numbers,
                "text": "New offers available! Visit our site for details.",
            }
            response = requests.post(
                endpoint, json=payload, headers=headers, timeout=30
            )
            response.raise_for_status()
        except Exception as exc:
            raise AirflowException(f"SMS campaign failed: {exc}")

    # ------------------------------------------------------------------
    # Task flow definition
    # ------------------------------------------------------------------
    customer_records = load_customer_segment_csv()
    email_task = send_email_campaign(customer_records)
    push_task = send_push_notification(customer_records)
    sms_task = send_sms_campaign(customer_records)

    # Fan‑out: all campaigns start after CSV load completes
    customer_records >> [email_task, push_task, sms_task]