import json
import os
import smtplib
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import requests
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

BASE_DATA_DIR = '/tmp/mahidol_aqi'
os.makedirs(BASE_DATA_DIR, exist_ok=True)

HTML_FILE_PATH = os.path.join(BASE_DATA_DIR, 'mahidol_aqi_{timestamp}.html')
JSON_FILE_PATH = os.path.join(BASE_DATA_DIR, 'mahidol_aqi_{timestamp}.json')

POSTGRES_CONN_ID = 'postgres_default'

SMTP_SERVER = 'smtp.gmail.com'
SMTP_PORT = 587
EMAIL_SENDER = 'your-email@gmail.com'
EMAIL_PASSWORD = 'your-app-password'
EMAIL_RECIPIENTS = ['recipient@example.com']

AQI_ALERT_THRESHOLD = 100

MAHIDOL_AQI_URL = 'https://www.example-mahidol-aqi.com'


def fetch_mahidol_aqi_html(**context):
    """Downloads the latest AQI