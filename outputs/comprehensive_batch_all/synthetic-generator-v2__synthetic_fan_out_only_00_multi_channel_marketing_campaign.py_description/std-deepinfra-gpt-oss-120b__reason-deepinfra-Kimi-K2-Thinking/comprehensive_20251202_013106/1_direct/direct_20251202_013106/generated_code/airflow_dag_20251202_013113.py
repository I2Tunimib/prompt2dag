import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator


def load_customer_segment_csv(**kwargs):
    """
    Load customer segment data from a CSV file and push it to XCom.
    """
    file_path = "/path/to/customer_segments.csv"  # Adjust path as needed
    if not os.path.isfile(file_path):
        logging.warning("Customer segment file %s not found. Returning empty list.", file_path)
        records = []
    else:
        df = pd.read_csv(file_path)
        logging.info("Loaded %d rows from %s.", len(df), file_path)
        records = df.to_dict(orient="records")
    kwargs["ti"].xcom_push(key="customer_data", value=records)


def send_email_campaign(**kwargs):
    """
    Send email campaign to premium customers.
    """
    data = kwargs["ti"].xcom_pull(key="customer_data", task_ids="load_customer_segment_csv") or []
    premium_customers = [row for row in data if row.get("segment") == "premium"]
    logging.info("Sending email campaign to %d premium customers.", len(premium_customers))
    # Insert real email sending logic here


def send_sms_campaign(**kwargs):
    """
    Send SMS campaign to all customers in the segment.
    """
    data = kwargs["ti"].xcom_pull(key="customer_data", task_ids="load_customer_segment_csv") or []
    logging.info("Sending SMS campaign to %d customers.", len(data))
    # Insert real SMS sending logic here


def send_push_notification(**kwargs):
    """
    Send push notification campaign to mobile app users.
    """
    data = kwargs["ti"].xcom_pull(key="customer_data", task_ids="load_customer_segment_csv") or []
    mobile_users = [row for row in data if row.get("contact_method") == "push"]
    logging.info("Sending push notification to %d mobile app users.", len(mobile_users))
    # Insert real push notification logic here


default_args = {
    "owner": "Marketing team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="marketing_campaign_fan_out",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    concurrency=3,
    tags=["marketing", "fan-out"],
) as dag:
    load_data = PythonOperator(
        task_id="load_customer_segment_csv",
        python_callable=load_customer_segment_csv,
    )

    email_campaign = PythonOperator(
        task_id="send_email_campaign",
        python_callable=send_email_campaign,
    )

    sms_campaign = PythonOperator(
        task_id="send_sms_campaign",
        python_callable=send_sms_campaign,
    )

    push_campaign = PythonOperator(
        task_id="send_push_notification",
        python_callable=send_push_notification,
    )

    load_data >> [email_campaign, sms_campaign, push_campaign]