from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'Marketing team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='marketing_campaign_fanout',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args,
    tags=['marketing', 'campaign'],
) as dag:

    def load_customer_segment_csv():
        """Load customer segment data from CSV file."""
        print("Loading customer segment data from CSV...")

    def send_email_campaign():
        """Send email campaign to premium customer segment."""
        print("Sending email campaign to premium customers...")

    def send_sms_campaign():
        """Send SMS campaign to customer segment."""
        print("Sending SMS campaign...")

    def send_push_notification():
        """Send push notification campaign to mobile app users."""
        print("Sending push notification campaign...")

    load_customer_data = PythonOperator(
        task_id='load_customer_segment_csv',
        python_callable=load_customer_segment_csv,
    )

    email_campaign = PythonOperator(
        task_id='send_email_campaign',
        python_callable=send_email_campaign,
    )

    sms_campaign = PythonOperator(
        task_id='send_sms_campaign',
        python_callable=send_sms_campaign,
    )

    push_campaign = PythonOperator(
        task_id='send_push_notification',
        python_callable=send_push_notification,
    )

    load_customer_data >> [email_campaign, sms_campaign, push_campaign]