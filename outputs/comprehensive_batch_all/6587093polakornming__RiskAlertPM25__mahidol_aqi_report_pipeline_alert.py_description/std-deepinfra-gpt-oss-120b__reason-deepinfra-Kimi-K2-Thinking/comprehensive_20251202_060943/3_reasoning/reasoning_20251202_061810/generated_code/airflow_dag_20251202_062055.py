"""
Mahidol University AQI ETL Pipeline

This DAG scrapes air quality data from Mahidol University's website,
processes it, loads it into a PostgreSQL star schema, and sends
conditional email alerts based on AQI thresholds.
"""

from datetime import datetime, timedelta
import json
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import requests
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowSkipException


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='mahidol_aqi_pipeline',
    default_args=default_args,
    description='ETL pipeline for Mahidol University AQI data with conditional alerts',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['aqi', 'mahidol', 'etl', 'environment'],
) as dag:

    def fetch_mahidol_aqi_html(**context):
        """Fetch AQI HTML from Mahidol University website and save locally."""
        # CONFIGURATION: Update with actual Mahidol AQI report URL
        url = "https://airquality.mahidol.ac.th/report"
        
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Create temp directory for pipeline files
        os.makedirs("/tmp/mahidol_aqi", exist_ok=True)
        file_path = "/tmp/mahidol_aqi/latest.html"
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        # Push metadata to XCom for downstream tasks
        ti = context['task_instance']
        ti.xcom_push(key='html_file_path', value=file_path)
        ti.xcom_push(key='fetch_timestamp', value=datetime.now().isoformat())

    def extract_and_validate_data(**context):
        """Parse HTML, validate data freshness, check duplicates, and create JSON."""
        ti = context['task_instance']
        
        # Retrieve HTML file path from upstream task
        html_file_path = ti.xcom_pull(task_ids='fetch_mahidol_aqi_html', key='html_file_path')
        
        if not html_file_path or not os.path.exists(html_file_path):
            raise FileNotFoundError(f"HTML file not found: {html_file_path}")
        
        # Parse HTML content
        with open(html_file_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f.read(), 'html.parser')
        
        # EXTRACTION LOGIC: Customize based on actual HTML structure
        # Example: Extract data from specific divs/tables
        data = {
            'timestamp': datetime.now().isoformat(),
            'location': 'Mahidol University',
            'aqi': 85,  # Parse actual AQI value from soup
            'pm25': 25.5,
            'pm10': 35.2,
            'o3': 45.1,
            'no2': 20.3,
            'so2': 10.1,
            'co': 0.8
        }
        
        # Validate data freshness (customize logic as needed)
        # Example: check if data is from current hour
        data_time = datetime.fromisoformat(data['timestamp'])
        if (datetime.now() - data_time).total_seconds() > 3600:
            raise ValueError("Data is older than 1 hour")
        
        # Check for duplicates in PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        try:
            # Check for existing record for this location and date
            cursor.execute(
                """
                SELECT COUNT(*) 
                FROM aqi_facts af
                JOIN location