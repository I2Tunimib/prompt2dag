from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from typing import Dict, List


default_args = {
    'owner': 'Marketing team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}


def load_customer_segment_csv() -> Dict[str, List[str]]:
    """Load customer segment data from CSV file."""
    print("Loading customer segment data from CSV...")
    customer_data = {
        'premium': ['premium_customer1@example.com', 'premium_customer2@example.com'],
        'standard': ['standard_customer1@example.com', 'standard_customer2@example.com'],
        'mobile_app_users': ['device_token_1', 'device_token_2']
    }
    print(f"Loaded {len(customer_data)} customer segments")
    return customer_data


def send_email_campaign() -> None:
    """Send email campaign to premium customer segment."""
    print("Sending email campaign to premium customers...")
    premium_emails = ['premium_customer1@example.com', 'premium_customer2@example.com']
    for email in premium_emails:
        print(f"Email sent to: {email}")
    print("Email campaign completed")


def send_sms_campaign() -> None:
    """Send SMS campaign to customer segment."""
    print("Sending SMS campaign to customers...")
    phone_numbers = ['+1234567890', '+0987654321']
    for phone in phone_numbers:
        print(f"SMS sent to: {phone}")
    print("SMS campaign completed")


def send_push_notification() -> None:
    """Send push notification campaign to mobile app users."""
    print("Sending push notification campaign to mobile app users...")
    device_tokens = ['device_token_1', 'device_token_2']
    for token in device_tokens:
        print(f"Push notification sent to device: {token}")
    print("Push notification campaign completed")


with DAG(
    dag_id='marketing_campaign_fanout',
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    tags=['marketing', 'campaign'],
    max_active_runs=1,
    max_active_tasks=3,
) as dag:
    
    load_task = PythonOperator(
        task_id='load_customer_segment_csv',
        python_callable=load_customer_segment_csv,
    )
    
    email_task = PythonOperator(
        task_id='send_email_campaign',
        python_callable=send_email_campaign,
    )
    
    sms_task = PythonOperator(
        task_id='send_sms_campaign',
        python_callable=send_sms_campaign,
    )
    
    push_task = PythonOperator(
        task_id='send_push_notification',
        python_callable=send_push_notification,
    )
    
    load_task >> [email_task, sms_task, push_task]