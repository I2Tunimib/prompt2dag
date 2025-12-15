import json
import smtplib
from datetime import datetime, timedelta
from email.message import EmailMessage
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def fetch_mahidol_aqi_html(**context):
    """
    Download the Mahidol University AQI report page and store it locally.
    """
    url = "https://www.mahidol.ac.th/aqi"  # Example URL; replace with actual endpoint
    response = requests.get(url, timeout=10)
    response.raise_for_status()

    html_path = Path("/tmp/mah_aqi.html")
    html_path.write_text(response.text, encoding="utf-8")
    context["ti"].xcom_push(key="html_path", value=str(html_path))


def extract_and_validate_data(**context):
    """
    Parse the downloaded HTML, validate freshness, check for duplicates,
    and write a structured JSON file.
    """
    html_path = Path(context["ti"].xcom_pull(key="html_path", task_ids="fetch_mahidol_aqi_html"))
    if not html_path.exists():
        raise FileNotFoundError(f"HTML file not found at {html_path}")

    soup = BeautifulSoup(html_path.read_text(encoding="utf-8"), "html.parser")

    # Example extraction logic – adapt to actual page structure
    aqi_value = int(soup.find("span", {"id": "aqi-value"}).text.strip())
    report_datetime_str = soup.find("meta", {"name": "report-date"})["content"]
    report_datetime = datetime.strptime(report_datetime_str, "%Y-%m-%d %H:%M:%S")

    # Freshness check: only process today's data
    if report_datetime.date() != datetime.utcnow().date():
        raise AirflowSkipException("Report is not from today; skipping pipeline.")

    # Duplicate check against PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    sql = "SELECT 1 FROM aqi_facts WHERE report_timestamp = %s LIMIT 1;"
    records = pg_hook.get_records(sql, parameters=(report_datetime,))
    if records:
        raise AirflowSkipException("Data for this timestamp already exists; skipping.")

    # Build JSON payload
    payload = {
        "report_timestamp": report_datetime.isoformat(),
        "aqi": aqi_value,
        "location": "Mahidol University Campus",
        "source": "Mahidol AQI Webpage",
    }

    json_path = Path("/tmp/mah_aqi.json")
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    context["ti"].xcom_push(key="json_path", value=str(json_path))


def load_data_to_postgres(**context):
    """
    Load the JSON data into PostgreSQL tables.
    """
    json_path = Path(context["ti"].xcom_pull(key="json_path", task_ids="extract_and_validate_data"))
    if not json_path.exists():
        raise FileNotFoundError(f"JSON file not found at {json_path}")

    data = json.loads(json_path.read_text(encoding="utf-8"))

    pg_hook = PostgresHook(postgres_conn_id="postgres_default")
    insert_datetime_sql = """
        INSERT INTO datetime_dim (report_timestamp)
        VALUES (%s)
        ON CONFLICT DO NOTHING;
    """
    insert_location_sql = """
        INSERT INTO location_dim (location_name)
        VALUES (%s)
        ON CONFLICT DO NOTHING;
    """
    insert_aqi_sql = """
        INSERT INTO aqi_facts (report_timestamp, aqi, location_name, source)
        VALUES (%s, %s, %s, %s);
    """

    report_ts = data["report_timestamp"]
    aqi = data["aqi"]
    location = data["location"]
    source = data["source"]

    pg_hook.run(insert_datetime_sql, parameters=(report_ts,))
    pg_hook.run(insert_location_sql, parameters=(location,))
    pg_hook.run(insert_aqi_sql, parameters=(report_ts, aqi, location, source))


def conditional_email_alert(**context):
    """
    Send an email alert if AQI exceeds the defined safety threshold.
    """
    json_path = Path(context["ti"].xcom_pull(key="json_path", task_ids="extract_and_validate_data"))
    if not json_path.exists():
        raise FileNotFoundError(f"JSON file not found at {json_path}")

    data = json.loads(json_path.read_text(encoding="utf-8"))
    aqi = data["aqi"]
    threshold = int(Variable.get("aqi_alert_threshold", default_var="100"))

    if aqi <= threshold:
        # No alert needed
        return

    # Email configuration
    gmail_user = Variable.get("gmail_user")
    gmail_password = Variable.get("gmail_password")
    recipients = Variable.get("alert_recipients", default_var="").split(",")

    if not recipients:
        raise ValueError("No email recipients configured in 'alert_recipients' variable.")

    subject = f"⚠️ AQI Alert: Current AQI = {aqi}"
    body = f"""\
Dear recipient,

The latest AQI reading from Mahidol University is {aqi}, which exceeds the safety threshold of {threshold}.

Please take appropriate precautions.

Best regards,
Air Quality Monitoring System
"""

    msg = EmailMessage()
    msg["From"] = gmail_user
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject
    msg.set_content(body)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(gmail_user, gmail_password)
        server.send_message(msg)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="mah_aqi_etl",
    default_args=default_args,
    description="ETL pipeline for Mahidol University AQI data",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["air_quality", "etl"],
) as dag:

    fetch_html = PythonOperator(
        task_id="fetch_mahidol_aqi_html",
        python_callable=fetch_mahidol_aqi_html,
        provide_context=True,
    )

    extract_validate = PythonOperator(
        task_id="extract_and_validate_data",
        python_callable=extract_and_validate_data,
        provide_context=True,
    )

    load_to_pg = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=load_data_to_postgres,
        provide_context=True,
    )

    send_alert = PythonOperator(
        task_id="conditional_email_alert",
        python_callable=conditional_email_alert,
        provide_context=True,
    )

    fetch_html >> extract_validate >> load_to_pg >> send_alert