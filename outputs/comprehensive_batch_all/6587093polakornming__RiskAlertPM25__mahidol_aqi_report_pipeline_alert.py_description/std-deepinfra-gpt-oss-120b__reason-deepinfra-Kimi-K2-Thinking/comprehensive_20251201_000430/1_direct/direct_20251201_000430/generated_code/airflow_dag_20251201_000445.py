from __future__ import annotations

import json
import os
import smtplib
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict

import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ----------------------------------------------------------------------
# Default arguments for the DAG
# ----------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

# ----------------------------------------------------------------------
# DAG definition
# ----------------------------------------------------------------------
with DAG(
    dag_id="mahidol_aqi_etl",
    default_args=default_args,
    description="ETL pipeline that fetches Mahidol University AQI data, loads it into PostgreSQL, and sends alerts.",
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "air_quality"],
) as dag:

    def fetch_html(**context: Dict[str, Any]) -> None:
        """
        Download the Mahidol AQI HTML report and store it locally.
        """
        url = "https://mahidol.ac.th/aqi-report"  # placeholder URL
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        html_path = Path("/tmp/mahidol_aqi.html")
        html_path.write_text(response.text, encoding="utf-8")
        context["ti"].xcom_push(key="html_path", value=str(html_path))

    fetch_html_task = PythonOperator(
        task_id="fetch_mahidol_aqi_html",
        python_callable=fetch_html,
    )

    def extract_and_validate(**context: Dict[str, Any]) -> None:
        """
        Parse the downloaded HTML, validate freshness, check for duplicates,
        and write a structured JSON file.
        """
        ti = context["ti"]
        html_path = Path(ti.xcom_pull(key="html_path", task_ids="fetch_mahidol_aqi_html"))
        if not html_path.is_file():
            raise FileNotFoundError(f"HTML file not found at {html_path}")

        soup = BeautifulSoup(html_path.read_text(encoding="utf-8"), "html.parser")

        # Example extraction logic – replace with actual selectors
        aqi_value_tag = soup.find("span", class_="aqi-value")
        datetime_tag = soup.find("time", class_="report-time")

        if not aqi_value_tag or not datetime_tag:
            raise ValueError("Required AQI data not found in HTML.")

        aqi_value = int(aqi_value_tag.text.strip())
        report_dt = datetime.strptime(datetime_tag["datetime"], "%Y-%m-%d %H:%M:%S")

        # Freshness check: only process if report is from today
        if report_dt.date() != datetime.utcnow().date():
            raise AirflowSkipException("Report is not from today; skipping processing.")

        # Duplicate check against PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        sql = "SELECT 1 FROM aqi_facts WHERE report_timestamp = %s LIMIT 1;"
        records = pg_hook.get_records(sql, parameters=(report_dt,))
        if records:
            # Duplicate found – skip downstream tasks
            ti.xcom_push(key="duplicate", value=True)
            raise AirflowSkipException("Duplicate record found; skipping load and alert.")

        # Build JSON payload
        payload = {
            "report_timestamp": report_dt.isoformat(),
            "aqi": aqi_value,
            "source": "Mahidol University",
        }

        json_path = Path("/tmp/mahidol_aqi.json")
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        ti.xcom_push(key="json_path", value=str(json_path))

    extract_task = PythonOperator(
        task_id="extract_and_validate_data",
        python_callable=extract_and_validate,
    )

    def load_to_postgres(**context: Dict[str, Any]) -> None:
        """
        Load the validated JSON data into PostgreSQL tables.
        """
        ti = context["ti"]
        if ti.xcom_pull(key="duplicate", task_ids="extract_and_validate_data"):
            raise AirflowSkipException("Duplicate flag set; skipping load.")

        json_path = Path(ti.xcom_pull(key="json_path", task_ids="extract_and_validate_data"))
        if not json_path.is_file():
            raise FileNotFoundError(f"JSON file not found at {json_path}")

        data = json.loads(json_path.read_text(encoding="utf-8"))
        report_ts = data["report_timestamp"]
        aqi = data["aqi"]

        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        insert_sql = """
            INSERT INTO aqi_facts (report_timestamp, aqi, source)
            VALUES (%s, %s, %s);
        """
        pg_hook.run(insert_sql, parameters=(report_ts, aqi, data["source"]))

    load_task = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=load_to_postgres,
    )

    def conditional_email_alert(**context: Dict[str, Any]) -> None:
        """
        Send an email alert if AQI exceeds the defined safety threshold.
        """
        ti = context["ti"]
        if ti.xcom_pull(key="duplicate", task_ids="extract_and_validate_data"):
            raise AirflowSkipException("Duplicate flag set; skipping email alert.")

        json_path = Path(ti.xcom_pull(key="json_path", task_ids="extract_and_validate_data"))
        data = json.loads(json_path.read_text(encoding="utf-8"))
        aqi = data["aqi"]

        # Safety threshold – can be moved to a Variable or Connection
        threshold = 100
        if aqi <= threshold:
            raise AirflowSkipException(f"AQI ({aqi}) is within safe limits; no alert sent.")

        # Load recipients from a JSON config file
        recipients_path = Path("/opt/airflow/config/email_recipients.json")
        if recipients_path.is_file():
            recipients = json.loads(recipients_path.read_text(encoding="utf-8")).get("recipients", [])
        else:
            # Fallback hard‑coded list
            recipients = ["alert@example.com"]

        subject = f"⚠️ Air Quality Alert – AQI {aqi}"
        body = f"""\
Dear Recipient,

The latest AQI report from Mahidol University indicates a value of {aqi},
which exceeds the safety threshold of {threshold}.

Report Time: {data['report_timestamp']}

Please take appropriate actions.

Best regards,
Air Quality Monitoring System
"""

        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = "no-reply@example.com"
        msg["To"] = ", ".join(recipients)

        # SMTP configuration – assumes a connection named "smtp_default" is defined in Airflow
        smtp_host = "smtp.gmail.com"
        smtp_port = 587
        smtp_user = "your_gmail_account@gmail.com"
        smtp_password = "your_app_password"

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(msg["From"], recipients, msg.as_string())

    email_task = PythonOperator(
        task_id="conditional_email_alert",
        python_callable=conditional_email_alert,
    )

    # ------------------------------------------------------------------
    # Set task dependencies (linear flow)
    # ------------------------------------------------------------------
    fetch_html_task >> extract_task >> load_task >> email_task