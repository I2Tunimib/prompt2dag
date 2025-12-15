from datetime import datetime, timedelta
import json
import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import requests
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def fetch_mahidol_aqi_html(**context):
    """Fetch AQI HTML from Mahidol University and save to file."""
    url = "https://airthailand.net/report.php"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    file_path = f"/tmp/mahidol_aqi_{context['ds_nodash']}.html"
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(response.text)
    
    context['task_instance'].xcom_push(key='html_file_path', value=file_path)
    return file_path

def extract_and_validate_data(**context):
    """Extract data from HTML, validate freshness, check duplicates, and save JSON."""
    ti = context['task_instance']
    html_file_path = ti.xcom_pull(task_ids='fetch_mahidol_aqi_html', key='html_file_path')
    
    with open(html_file_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')
    
    # Extract sample data (adjust selectors based on actual HTML structure)
    data_table = soup.find('table', {'id': 'aqi-table'})
    if not data_table:
        raise ValueError("AQI table not found in HTML")
    
    rows = data_table.find_all('tr')[1:]  # Skip header
    if not rows:
        raise AirflowSkipException("No data rows found in HTML")
    
    # Extract first row as example
    cells = rows[0].find_all('td')
    if len(cells) < 5:
        raise ValueError("Insufficient data cells in table row")
    
    report_date = cells[0].get_text(strip=True)
    location = cells[1].get_text(strip=True)
    aqi_value = int(cells[2].get_text(strip=True))
    pm25 = float(cells[3].get_text(strip=True))
    pm10 = float(cells[4].get_text(strip=True))
    
    # Validate freshness (example: check if date matches execution date)
    exec_date = context['ds']
    if report_date != exec_date:
        raise AirflowSkipException(f"Data freshness check failed: {report_date} != {exec_date}")
    
    # Check for duplicates
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    exists = pg_hook.get_first(
        "SELECT 1 FROM aqi_facts WHERE report_date = %s AND location = %s",
        parameters=(report_date, location)
    )
    if exists:
        raise AirflowSkipException(f"Data already exists for {report_date} - {location}")
    
    # Create structured JSON
    data = {
        'report_date': report_date,
        'location': location,
        'aqi': aqi_value,
        'pm25': pm25,
        'pm10': pm10,
        'timestamp': datetime.now().isoformat()
    }
    
    json_file_path = f"/tmp/mahidol_aqi_{context['ds_nodash']}.json"
    with open(json_file_path, 'w') as f:
        json.dump(data, f)
    
    ti.xcom_push(key='json_file_path', value=json_file_path)
    ti.xcom_push(key='aqi_value', value=aqi_value)
    return json_file_path

def load_data_to_postgresql(**context):
    """Load JSON data into PostgreSQL dimensional model."""
    ti = context['task_instance']
    json_file_path = ti.xcom_pull(task_ids='extract_and_validate_data', key='json_file_path')
    
    with open(json_file_path, 'r') as f:
        data = json.load(f)
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Insert datetime dimension
    pg_hook.run(
        """
        INSERT INTO datetime_dim (report_date, year, month, day)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (report_date) DO NOTHING
        """,
        parameters=(
            data['report_date'],
            int(data['report_date'][:4]),
            int(data['report_date'][5:7]),
            int(data['report_date'][8:10])
        )
    )
    
    # Insert location dimension
    pg_hook.run(
        """
        INSERT INTO location_dim (location_name)
        VALUES (%s)
        ON CONFLICT (location_name) DO NOTHING
        """,
        parameters=(data['location'],)
    )
    
    # Insert pollution dimension
    pg_hook.run(
        """
        INSERT INTO pollution_dim (pm25, pm10)
        VALUES (%s, %s)
        ON CONFLICT (pm25, pm10) DO NOTHING
        """,
        parameters=(data['pm25'], data['pm10'])
    )
    
    # Insert AQI fact
    pg_hook.run(
        """
        INSERT INTO aqi_facts (report_date, location, pm25, pm10, aqi_value)
        VALUES (%s, %s, %s, %s, %s)
        """,
        parameters=(
            data['report_date'],
            data['location'],
            data['pm25'],
            data['pm10'],
            data['aqi']
        )
    )

def conditional_email_alert(**context):
    """Send email alert if AQI exceeds threshold."""
    ti = context['task_instance']
    aqi_value = ti.xcom_pull(task_ids='extract_and_validate_data', key='aqi_value')
    
    # Threshold from Airflow Variable or default
    from airflow.models import Variable
    threshold = int(Variable.get('aqi_alert_threshold', default_var=100))
    
    if aqi_value <= threshold:
        raise AirflowSkipException(f"AQI {aqi_value} is within safe threshold ({threshold})")
    
    # Email configuration
    smtp_conn_id = 'smtp_default'
    conn = PostgresHook.get_connection(smtp_conn_id)
    
    subject = f"Air Quality Alert: AQI {aqi_value} Exceeds Safe Level"
    body = f"""
    <html>
    <body>
        <h2>Air Quality Alert</h2>
        <p><strong>AQI Value:</strong> {aqi_value}</p>
        <p><strong>Threshold:</strong> {threshold}</p>
        <p><strong>Date:</strong> {context['ds']}</p>
        <p>Air quality has exceeded safe levels. Please take necessary precautions.</p>
    </body>
    </html>
    """
    
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = conn.login
    msg['To'] = Variable.get('aqi_alert_recipients', default_var='admin@example.com')
    msg.attach(MIMEText(body, 'html'))
    
    with smtplib.SMTP(conn.host, conn.port) as server:
        if conn.password:
            server.starttls()
            server.login(conn.login, conn.password)
        server.send_message(msg)

with DAG(
    'mahidol_aqi_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Mahidol University AQI data with conditional alerts',
    schedule_interval='0 6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['aqi', 'mahidol', 'etl', 'environment'],
) as dag:
    
    fetch_task = PythonOperator(
        task_id='fetch_mahidol_aqi_html',
        python_callable=fetch_mahidol_aqi_html,
    )
    
    extract_task = PythonOperator(
        task_id='extract_and_validate_data',
        python_callable=extract_and_validate_data,
    )
    
    load_task = PythonOperator(
        task_id='load_data_to_postgresql',
        python_callable=load_data_to_postgresql,
    )
    
    alert_task = PythonOperator(
        task_id='conditional_email_alert',
        python_callable=conditional_email_alert,
    )
    
    fetch_task >> extract_task >> load_task >> alert_task