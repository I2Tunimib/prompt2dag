from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.smtp.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import get_current_context
from airflow.models import Variable
import requests
from bs4 import BeautifulSoup
import json
import os
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'mahidol_aqi_pipeline',
    default_args=default_args,
    description='ETL pipeline for Mahidol University AQI data',
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(1),
    catchup=False,
) as dag:

    def fetch_mahidol_aqi_html():
        url = Variable.get('mahidol_aqi_url')
        response = requests.get(url)
        response.raise_for_status()
        html_content = response.text
        with open('/tmp/aqi_report.html', 'w') as file:
            file.write(html_content)

    fetch_html = PythonOperator(
        task_id='fetch_mahidol_aqi_html',
        python_callable=fetch_mahidol_aqi_html,
    )

    def extract_and_validate_data():
        with open('/tmp/aqi_report.html', 'r') as file:
            html_content = file.read()
        soup = BeautifulSoup(html_content, 'html.parser')
        aqi_data = parse_aqi_data(soup)
        if not is_data_fresh(aqi_data):
            raise ValueError("Data is not fresh")
        if is_data_duplicate(aqi_data):
            raise ValueError("Data is a duplicate")
        with open('/tmp/aqi_data.json', 'w') as file:
            json.dump(aqi_data, file)

    def parse_aqi_data(soup):
        # Placeholder for actual parsing logic
        return {
            'datetime': datetime.now().isoformat(),
            'location': 'Bangkok',
            'aqi': 50,
            'pollutants': [
                {'name': 'PM2.5', 'value': 30},
                {'name': 'PM10', 'value': 50},
            ]
        }

    def is_data_fresh(aqi_data):
        # Placeholder for actual freshness check
        return True

    def is_data_duplicate(aqi_data):
        # Placeholder for actual duplicate check
        return False

    extract_data = PythonOperator(
        task_id='extract_and_validate_data',
        python_callable=extract_and_validate_data,
    )

    def load_data_to_postgres():
        with open('/tmp/aqi_data.json', 'r') as file:
            aqi_data = json.load(file)
        # Placeholder for actual data loading logic
        pass

    load_data = PythonOperator(
        task_id='load_data_to_postgres',
        python_callable=load_data_to_postgres,
    )

    def send_email_alert():
        context = get_current_context()
        aqi_data = context['ti'].xcom_pull(task_ids='extract_and_validate_data')
        aqi_value = aqi_data['aqi']
        if aqi_value > int(Variable.get('aqi_threshold', default_var=100)):
            recipients = Variable.get('email_recipients', default_var='user@example.com')
            subject = 'Air Quality Alert'
            body = f"The current AQI value is {aqi_value}, which exceeds the safe threshold."
            return EmailOperator(
                task_id='send_email_alert',
                to=recipients,
                subject=subject,
                html_content=body,
            ).execute(context)
        else:
            return DummyOperator(task_id='no_alert_needed').execute(context)

    send_alert = PythonOperator(
        task_id='send_email_alert',
        python_callable=send_email_alert,
    )

    # Define task dependencies
    fetch_html >> extract_data >> load_data >> send_alert