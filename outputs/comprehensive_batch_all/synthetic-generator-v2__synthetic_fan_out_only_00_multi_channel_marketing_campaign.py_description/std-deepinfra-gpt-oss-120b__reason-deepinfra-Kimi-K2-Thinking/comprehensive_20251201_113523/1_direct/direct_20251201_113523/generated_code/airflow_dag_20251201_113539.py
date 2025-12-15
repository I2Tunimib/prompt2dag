"""Multi-channel marketing campaign DAG.

This DAG loads customer segment data from a CSV file and then runs three
parallel marketing campaigns (email, SMS, push notification). It is
scheduled to run daily starting on 2024-01-01.
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator


def load_customer_segment_csv(**kwargs):
    """Load customer segment data from a CSV file.

    In a real implementation this would read a CSV from a storage location
    and push the data to XCom for downstream tasks.
    """
    logging.info("Loading customer segment CSV file.")
    # Placeholder logic – replace with actual file reading.
    customer_data = {"example_key": "example_value"}
    # Push data to XCom for downstream tasks if needed.
    kwargs["ti"].xcom_push(key="customer_data", value=customer_data)
    return customer_data


def send_email_campaign(**kwargs):
    """Send email campaign to premium customer segment."""
    logging.info("Sending email campaign to premium customers.")
    # Retrieve data from XCom if required.
    customer_data = kwargs["ti"].xcom_pull(key="customer_data", task_ids="load_customer_segment_csv")
    # Placeholder for email sending logic.
    logging.debug("Customer data for email: %s", customer_data)
    return "email_sent"


def send_sms_campaign(**kwargs):
    """Send SMS campaign to the customer segment."""
    logging.info("Sending SMS campaign to customers.")
    customer_data = kwargs["ti"].xcom_pull(key="customer_data", task_ids="load_customer_segment_csv")
    logging.debug("Customer data for SMS: %s", customer_data)
    # Placeholder for SMS sending logic.
    return "sms_sent"


def send_push_notification(**kwargs):
    """Send push notification campaign to mobile app users."""
    logging.info("Sending push notification campaign to mobile app users.")
    customer_data = kwargs["ti"].xcom_pull(key="customer_data", task_ids="load_customer_segment_csv")
    logging.debug("Customer data for push notifications: %s", customer_data)
    # Placeholder for push notification logic.
    return "push_sent"


default_args = {
    "owner": "Marketing team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="multi_channel_marketing_campaign",
    default_args=default_args,
    description="Daily fan‑out marketing campaign DAG",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=3,
    tags=["marketing"],
) as dag:
    load_customer_segment = PythonOperator(
        task_id="load_customer_segment_csv",
        python_callable=load_customer_segment_csv,
        provide_context=True,
    )

    email_campaign = PythonOperator(
        task_id="send_email_campaign",
        python_callable=send_email_campaign,
        provide_context=True,
    )

    sms_campaign = PythonOperator(
        task_id="send_sms_campaign",
        python_callable=send_sms_campaign,
        provide_context=True,
    )

    push_campaign = PythonOperator(
        task_id="send_push_notification",
        python_callable=send_push_notification,
        provide_context=True,
    )

    # Define fan‑out dependencies
    load_customer_segment >> [email_campaign, sms_campaign, push_campaign]