from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Marketing team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def load_customer_segment_csv():
    """Load customer segment data from CSV file."""
    print("Loading customer segment data from CSV...")
    return "Customer data loaded successfully"


def send_email_campaign():
    """Send email campaign to premium customer segment."""
    print("Sending email campaign to premium customers...")
    return "Email campaign sent"


def send_sms_campaign():
    """Send SMS campaign to customer segment."""
    print("Sending SMS campaign...")
    return "SMS campaign sent"


def send_push_notification():
    """Send push notification campaign to mobile app users."""
    print("Sending push notification campaign...")
    return "Push notification campaign sent"


with DAG(
    dag_id='multi_channel_marketing_campaign',
    description='Multi-channel marketing campaign with fan-out pattern',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    concurrency=3,
    max_active_runs=1,
    tags=['marketing', 'campaign'],
) as dag:

    load_customer_data = PythonOperator(
        task_id='load_customer_segment_csv',
        python_callable=load_customer_segment_csv,
    )

    send_email = PythonOperator(
        task_id='send_email_campaign',
        python_callable=send_email_campaign,
    )

    send_sms = PythonOperator(
        task_id='send_sms_campaign',
        python_callable=send_sms_campaign,
    )

    send_push = PythonOperator(
        task_id='send_push_notification',
        python_callable=send_push_notification,
    )

    # Fan-out pattern: load data then run all campaigns in parallel
    load_customer_data >> [send_email, send_sms, send_push]