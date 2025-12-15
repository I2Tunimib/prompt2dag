import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator


def load_customer_segment_csv(**kwargs):
    """
    Load customer segment data from a CSV file.

    The CSV is expected to contain columns such as:
    - customer_id
    - segment
    - email
    - phone
    - device_id
    """
    csv_path = "/opt/airflow/data/customer_segments.csv"
    try:
        df = pd.read_csv(csv_path)
        logging.info("Loaded %d rows from %s", len(df), csv_path)
        # Push dataframe to XCom for downstream tasks if needed
        kwargs["ti"].xcom_push(key="customer_data", value=df.to_dict(orient="records"))
    except FileNotFoundError:
        logging.error("CSV file not found at %s", csv_path)
        raise


def send_email_campaign(**kwargs):
    """
    Send email campaign to premium customers.
    """
    records = kwargs["ti"].xcom_pull(key="customer_data", task_ids="load_customer_segment_csv")
    premium_customers = [r for r in records if r.get("segment") == "premium"]
    logging.info("Sending email campaign to %d premium customers", len(premium_customers))
    # Placeholder for actual email sending logic
    for customer in premium_customers:
        logging.debug("Email sent to %s", customer.get("email"))


def send_sms_campaign(**kwargs):
    """
    Send SMS campaign to customers in the target segment.
    """
    records = kwargs["ti"].xcom_pull(key="customer_data", task_ids="load_customer_segment_csv")
    target_customers = [r for r in records if r.get("segment") in ("standard", "premium")]
    logging.info("Sending SMS campaign to %d customers", len(target_customers))
    # Placeholder for actual SMS sending logic
    for customer in target_customers:
        logging.debug("SMS sent to %s", customer.get("phone"))


def send_push_notification(**kwargs):
    """
    Send push notification campaign to mobile app users.
    """
    records = kwargs["ti"].xcom_pull(key="customer_data", task_ids="load_customer_segment_csv")
    mobile_users = [r for r in records if r.get("device_id")]
    logging.info("Sending push notifications to %d mobile users", len(mobile_users))
    # Placeholder for actual push notification logic
    for user in mobile_users:
        logging.debug("Push notification sent to device %s", user.get("device_id"))


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