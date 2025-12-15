# -*- coding: utf-8 -*-
"""
Generated Airflow DAG for multi_channel_marketing_campaign
Author: Automated DAG Generator
Date: 2024-06-28
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

import pandas as pd
import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.decorators import task

# Default arguments applied to all tasks
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# DAG definition
with DAG(
    dag_id="multi_channel_marketing_campaign",
    description="Executes a multi-channel marketing campaign by loading customer segment data "
    "and triggering parallel email, SMS, and push notification campaigns.",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["marketing", "fanout"],
    max_active_runs=1,
    render_template_as_native_obj=True,
    timezone="UTC",
) as dag:

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def load_customer_segment_csv() -> List[Dict[str, Any]]:
        """
        Load the customer segment CSV from the local filesystem connection.
        Returns a list of dictionaries (records) to be shared via XCom.
        """
        conn_id = "local_customer_segment_filesystem"
        try:
            conn = BaseHook.get_connection(conn_id)
            # Assume the connection's host field contains the absolute directory path
            # and the schema field contains the CSV filename.
            directory = conn.host.rstrip("/")
            filename = conn.schema or "customer_segment.csv"
            file_path = f"{directory}/{filename}"
            logging.info("Loading customer segment CSV from %s", file_path)

            df = pd.read_csv(file_path)
            records = df.to_dict(orient="records")
            logging.info("Loaded %d customer records", len(records))
            return records
        except Exception as exc:
            logging.error("Failed to load customer segment CSV: %s", exc)
            raise AirflowException(f"Error loading CSV from {conn_id}") from exc

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def send_email_campaign(customers: List[Dict[str, Any]]) -> None:
        """
        Send email campaign using the Email Delivery Service API.
        """
        conn_id = "email_delivery_api"
        try:
            conn = BaseHook.get_connection(conn_id)
            endpoint = conn.host  # Expected to be the full URL for the email API
            auth = (conn.login, conn.password) if conn.login else None
            headers = {"Content-Type": "application/json"}

            payload = {"recipients": customers, "campaign": "daily_email"}
            logging.info("Posting email campaign to %s with %d recipients", endpoint, len(customers))
            response = requests.post(endpoint, json=payload, headers=headers, auth=auth, timeout=30)

            if not response.ok:
                raise AirflowException(
                    f"Email API responded with status {response.status_code}: {response.text}"
                )
            logging.info("Email campaign dispatched successfully.")
        except Exception as exc:
            logging.error("Failed to send email campaign: %s", exc)
            raise AirflowException("Error in send_email_campaign") from exc

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def send_sms_campaign(customers: List[Dict[str, Any]]) -> None:
        """
        Send SMS campaign using the SMS Gateway Service API.
        """
        conn_id = "sms_gateway_api"
        try:
            conn = BaseHook.get_connection(conn_id)
            endpoint = conn.host  # Expected to be the full URL for the SMS API
            auth = (conn.login, conn.password) if conn.login else None
            headers = {"Content-Type": "application/json"}

            payload = {"recipients": customers, "campaign": "daily_sms"}
            logging.info("Posting SMS campaign to %s with %d recipients", endpoint, len(customers))
            response = requests.post(endpoint, json=payload, headers=headers, auth=auth, timeout=30)

            if not response.ok:
                raise AirflowException(
                    f"SMS API responded with status {response.status_code}: {response.text}"
                )
            logging.info("SMS campaign dispatched successfully.")
        except Exception as exc:
            logging.error("Failed to send SMS campaign: %s", exc)
            raise AirflowException("Error in send_sms_campaign") from exc

    @task(retries=2, retry_delay=timedelta(minutes=5))
    def send_push_notification(customers: List[Dict[str, Any]]) -> None:
        """
        Send push notification campaign using the Push Notification Service API.
        """
        conn_id = "push_notification_api"
        try:
            conn = BaseHook.get_connection(conn_id)
            endpoint = conn.host  # Expected to be the full URL for the push API
            auth = (conn.login, conn.password) if conn.login else None
            headers = {"Content-Type": "application/json"}

            payload = {"recipients": customers, "campaign": "daily_push"}
            logging.info(
                "Posting push notification campaign to %s with %d recipients",
                endpoint,
                len(customers),
            )
            response = requests.post(endpoint, json=payload, headers=headers, auth=auth, timeout=30)

            if not response.ok:
                raise AirflowException(
                    f"Push API responded with status {response.status_code}: {response.text}"
                )
            logging.info("Push notification campaign dispatched successfully.")
        except Exception as exc:
            logging.error("Failed to send push notification campaign: %s", exc)
            raise AirflowException("Error in send_push_notification") from exc

    # Define task pipeline
    customer_data = load_customer_segment_csv()
    email_task = send_email_campaign(customer_data)
    sms_task = send_sms_campaign(customer_data)
    push_task = send_push_notification(customer_data)

    # Set fan‑out dependencies
    customer_data >> [email_task, sms_task, push_task]