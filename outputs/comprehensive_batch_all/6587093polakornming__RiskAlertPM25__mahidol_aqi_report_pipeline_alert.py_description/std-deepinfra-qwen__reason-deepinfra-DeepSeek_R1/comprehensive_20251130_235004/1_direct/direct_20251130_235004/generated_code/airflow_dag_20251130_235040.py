from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor
import requests
from bs4 import BeautifulSoup
import json
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def fetch_mahidol_aqi_html(**kwargs):
    url = 'https://mahidol-university-aqi-report.com/latest'
    response = requests.get(url)
    response.raise_for_status()
    html_content = response.text
    html_file_path = '/tmp/mahidol_aqi_report.html'
    with open(html_file_path, 'w') as file:
        file.write(html_content)
    return html_file_path

def extract_and_validate_data(**kwargs):
    ti = kwargs['ti']
    html_file_path = ti.xcom_pull(task_ids='fetch_mahidol_aqi_html')
    with open(html_file_path, 'r') as file:
        html_content = file.read()
    soup = BeautifulSoup(html_content, 'html.parser')
    aqi_data = parse_aqi_data(soup)
    if not is_data_fresh(aqi_data) or is_data_duplicate(aqi_data):
        return None
    json_data = transform_to_json(aqi_data)
    json_file_path = '/tmp/aqi_data.json'
    with open(json_file_path, 'w') as file:
        json.dump(json_data, file)
    return json_file_path

def parse_aqi_data(soup):
    # Placeholder for actual parsing logic
    aqi_data = {
        'datetime': '2023-10-01T12:00:00Z',
        'location': 'Bangkok',
        'aqi': 50,
        'pm25': 30,
        'pm10': 40
    }
    return aqi_data

def is_data_fresh(aqi_data):
    # Placeholder for data freshness check
    return True

def is_data_duplicate(aqi_data):
    # Placeholder for duplicate check against the database
    hook = PostgresHook(postgres_conn_id='postgres_default')
    sql = "SELECT * FROM aqi_data WHERE datetime = %s AND location = %s"
    result = hook.get_records(sql, parameters=(aqi_data['datetime'], aqi_data['location']))
    return len(result) > 0

def transform_to_json(aqi_data):
    # Placeholder for transformation logic
    return aqi_data

def load_data_to_postgres(**kwargs):
    ti = kwargs['ti']
    json_file_path = ti.xcom_pull(task_ids='extract_and_validate_data')
    if not json_file_path:
        return
    with open(json_file_path, 'r') as file:
        json_data = json.load(file)
    hook = PostgresHook(postgres_conn_id='postgres_default')
    insert_sql = """
    INSERT INTO aqi_data (datetime, location, aqi, pm25, pm10)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (datetime, location) DO NOTHING
    """
    hook.run(insert_sql, parameters=(
        json_data['datetime'], json_data['location'], json_data['aqi'], json_data['pm25'], json_data['pm10']
    ))

def conditional_email_alert(**kwargs):
    ti = kwargs['ti']
    json_file_path = ti.xcom_pull(task_ids='extract_and_validate_data')
    if not json_file_path:
        return
    with open(json_file_path, 'r') as file:
        json_data = json.load(file)
    if json_data['aqi'] > 100:
        return 'send_email_alert'
    return 'no_email_alert'

with DAG(
    'mahidol_aqi_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False
) as dag:

    fetch_html = PythonOperator(
        task_id='fetch_mahidol_aqi_html',
        python_callable=fetch_mahidol_aqi_html,
        provide_context=True
    )

    extract_data = PythonOperator(
        task_id='extract_and_validate_data',
        python_callable=extract_and_validate_data,
        provide_context=True
    )

    load_data = PythonOperator(
        task_id='load_data_to_postgres',
        python_callable=load_data_to_postgres,
        provide_context=True
    )

    conditional_alert = BranchPythonOperator(
        task_id='conditional_email_alert',
        python_callable=conditional_email_alert,
        provide_context=True
    )

    send_email = EmailOperator(
        task_id='send_email_alert',
        to='alert@example.com',
        subject='Air Quality Alert',
        html_content='The AQI has exceeded safe levels.'
    )

    no_email = DummyOperator(
        task_id='no_email_alert'
    )

    fetch_html >> extract_data >> load_data >> conditional_alert
    conditional_alert >> [send_email, no_email]