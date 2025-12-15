from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.smtp.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
from bs4 import BeautifulSoup
import json
import os

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

    def fetch_mahidol_aqi_html(**kwargs):
        url = 'https://example.com/mahidol-aqi-report'
        response = requests.get(url)
        response.raise_for_status()
        html_content = response.text
        html_file_path = '/tmp/mahidol_aqi_report.html'
        with open(html_file_path, 'w') as file:
            file.write(html_content)
        return html_file_path

    def extract_and_validate_data(**kwargs):
        html_file_path = kwargs['ti'].xcom_pull(task_ids='fetch_mahidol_aqi_html')
        with open(html_file_path, 'r') as file:
            html_content = file.read()
        soup = BeautifulSoup(html_content, 'html.parser')
        aqi_data = parse_aqi_data(soup)
        if not is_data_fresh(aqi_data) or is_data_duplicate(aqi_data):
            return None
        json_file_path = '/tmp/aqi_data.json'
        with open(json_file_path, 'w') as file:
            json.dump(aqi_data, file)
        return json_file_path

    def parse_aqi_data(soup):
        # Placeholder for actual parsing logic
        aqi_data = {
            'datetime': '2023-10-01T12:00:00Z',
            'location': 'Bangkok',
            'aqi': 50,
            'pollutants': [
                {'name': 'PM2.5', 'value': 10},
                {'name': 'PM10', 'value': 20},
            ]
        }
        return aqi_data

    def is_data_fresh(aqi_data):
        # Placeholder for actual freshness check logic
        return True

    def is_data_duplicate(aqi_data):
        # Placeholder for actual deduplication check logic
        return False

    def load_data_to_postgres(**kwargs):
        json_file_path = kwargs['ti'].xcom_pull(task_ids='extract_and_validate_data')
        if not json_file_path:
            return
        with open(json_file_path, 'r') as file:
            aqi_data = json.load(file)
        # Placeholder for actual data transformation and loading logic
        return aqi_data

    def send_email_alert(**kwargs):
        aqi_data = kwargs['ti'].xcom_pull(task_ids='load_data_to_postgres')
        if not aqi_data or aqi_data['aqi'] <= 100:
            return
        recipients = get_email_recipients()
        subject = 'Air Quality Alert: AQI Exceeds Safe Levels'
        body = f"The current AQI is {aqi_data['aqi']}, which exceeds safe levels."
        EmailOperator(
            task_id='send_email_alert',
            to=recipients,
            subject=subject,
            html_content=body,
        ).execute(context=kwargs)

    def get_email_recipients():
        # Placeholder for actual recipient retrieval logic
        return ['alert@example.com']

    # Define tasks
    fetch_html_task = PythonOperator(
        task_id='fetch_mahidol_aqi_html',
        python_callable=fetch_mahidol_aqi_html,
    )

    extract_data_task = PythonOperator(
        task_id='extract_and_validate_data',
        python_callable=extract_and_validate_data,
    )

    load_data_task = PythonOperator(
        task_id='load_data_to_postgres',
        python_callable=load_data_to_postgres,
    )

    send_alert_task = PythonOperator(
        task_id='send_email_alert',
        python_callable=send_email_alert,
    )

    # Define task dependencies
    fetch_html_task >> extract_data_task >> load_data_task >> send_alert_task