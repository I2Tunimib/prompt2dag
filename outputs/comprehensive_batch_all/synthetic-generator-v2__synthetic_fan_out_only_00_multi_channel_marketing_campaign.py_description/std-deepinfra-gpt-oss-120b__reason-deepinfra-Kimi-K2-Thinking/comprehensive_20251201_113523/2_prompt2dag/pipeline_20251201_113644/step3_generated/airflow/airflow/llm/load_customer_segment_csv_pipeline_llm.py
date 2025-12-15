# -*- coding: utf-8 -*-
"""
Generated Airflow DAG: load_customer_segment_csv_pipeline
Description: Comprehensive Pipeline Description
Pattern: fanout
Generation Timestamp: 2024-06-13T12:00:00Z
"""

import logging
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.decorators import task

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
    dag_id="load_customer_segment_csv_pipeline",
    description="Comprehensive Pipeline Description",
    schedule_interval=None,  # Disabled (no schedule)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["fanout", "customer", "segment"],
    max_active_runs=1,
    render_template_as_native_obj=True,
) as dag:

    @task(task_id="load_customer_segment_csv")
    def load_customer_segment_csv() -> pd.DataFrame:
        """
        Load the customer segment CSV from the local filesystem.
        Returns a pandas DataFrame for downstream tasks.
        """
        try:
            # Retrieve the filesystem connection (expects a path in the extra field)
            conn = BaseHook.get_connection("local_csv_filesystem")
            csv_path = conn.extra_dejson.get("path")
            if not csv_path:
                raise AirflowFailException("Filesystem connection missing 'path' in extra.")

            logging.info("Loading CSV from %s", csv_path)
            df = pd.read_csv(csv_path)
            logging.info("Loaded %d records.", len(df))
            return df
        except Exception as exc:
            logging.error("Failed to load CSV: %s", exc)
            raise AirflowFailException(f"load_customer_segment_csv failed: {exc}")

    @task(task_id="send_email_campaign")
    def send_email_campaign(df: pd.DataFrame):
        """
        Send an email campaign using the Email Delivery Service API.
        """
        try:
            conn = BaseHook.get_connection("email_delivery_api")
            endpoint = conn.host
            api_key = conn.password  # Assuming API key stored as password
            headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

            payload = {
                "subject": "Your Customer Segment Update",
                "recipients": df["email"].tolist(),
                "body": "Please find your segment details attached."
            }

            logging.info("Posting email campaign to %s", endpoint)
            response = requests.post(endpoint, json=payload, headers=headers, timeout=30)
            response.raise_for_status()
            logging.info("Email campaign sent successfully. Response: %s", response.text)
        except Exception as exc:
            logging.error("Email campaign failed: %s", exc)
            raise AirflowFailException(f"send_email_campaign failed: {exc}")

    @task(task_id="send_push_notification")
    def send_push_notification(df: pd.DataFrame):
        """
        Send push notifications using the Push Notification Service API.
        """
        try:
            conn = BaseHook.get_connection("push_notification_api")
            endpoint = conn.host
            api_key = conn.password
            headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

            payload = {
                "title": "Customer Segment Update",
                "message": "Your segment details are now available.",
                "device_tokens": df["push_token"].dropna().tolist()
            }

            logging.info("Posting push notification to %s", endpoint)
            response = requests.post(endpoint, json=payload, headers=headers, timeout=30)
            response.raise_for_status()
            logging.info("Push notification sent successfully. Response: %s", response.text)
        except Exception as exc:
            logging.error("Push notification failed: %s", exc)
            raise AirflowFailException(f"send_push_notification failed: {exc}")

    @task(task_id="send_sms_campaign")
    def send_sms_campaign(df: pd.DataFrame):
        """
        Send SMS campaign using the SMS Gateway API.
        """
        try:
            conn = BaseHook.get_connection("sms_gateway_api")
            endpoint = conn.host
            api_key = conn.password
            headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

            payload = {
                "message": "Your customer segment details are ready.",
                "recipients": df["phone_number"].dropna().tolist()
            }

            logging.info("Posting SMS campaign to %s", endpoint)
            response = requests.post(endpoint, json=payload, headers=headers, timeout=30)
            response.raise_for_status()
            logging.info("SMS campaign sent successfully. Response: %s", response.text)
        except Exception as exc:
            logging.error("SMS campaign failed: %s", exc)
            raise AirflowFailException(f"send_sms_campaign failed: {exc}")

    # -------------------------------------------------------------------------
    # Define task pipeline (fanout)
    # -------------------------------------------------------------------------
    df = load_customer_segment_csv()
    email = send_email_campaign(df)
    push = send_push_notification(df)
    sms = send_sms_campaign(df)

    # Set dependencies: downstream tasks wait for CSV load
    df >> [email, push, sms]