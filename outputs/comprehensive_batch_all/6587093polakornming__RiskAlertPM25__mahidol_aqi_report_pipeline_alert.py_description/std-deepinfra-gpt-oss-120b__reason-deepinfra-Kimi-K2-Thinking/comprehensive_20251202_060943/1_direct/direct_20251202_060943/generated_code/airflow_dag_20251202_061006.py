from __future__ import annotations

import json
import logging
import os
import smtplib
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Dict

import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python import PythonOperator

# ----------------------------------------------------------------------
# Default arguments for the DAG
# ----------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# ----------------------------------------------------------------------
# DAG definition
# ----------------------------------------------------------------------
with DAG(
    dag_id="mahidol_aqi_etl",
    default_args=DEFAULT_ARGS,
    description="ETL pipeline that fetches Mahidol University AQI data, loads it into PostgreSQL, and sends alerts.",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "air_quality"],
) as dag:

    def fetch_html(**context: Any) -> None:
        """
        Download the Mahidol University AQI HTML page and store it locally.
        The file path is pushed to XCom for downstream tasks.
        """
        url = "https://www.mahidol.ac.th/aqi"  # Example URL; replace with actual endpoint
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        output_dir = Path("/tmp/mahidol_aqi")
        output_dir.mkdir(parents=True, exist_ok=True)
        html_path = output_dir / "aqi_report.html"
        html_path.write_text(response.text, encoding="utf-8")

        logging.info("Saved AQI HTML to %s", html_path)
        context["ti"].xcom_push(key="html_path", value=str(html_path))

    fetch_html_task = PythonOperator(
        task_id="fetch_mahidol_aqi_html",
        python_callable=fetch_html,
        provide_context=True,
    )

    def extract_and_validate(**context: Any) -> None:
        """
        Parse the downloaded HTML, validate freshness, check for duplicates,
        and write a structured JSON file. Push JSON path and a flag indicating
        whether loading should be skipped.
        """
        ti = context["ti"]
        html_path = Path(ti.xcom_pull(key="html_path", task_ids="fetch_mahidol_aqi_html"))
        if not html_path.is_file():
            raise FileNotFoundError(f"HTML file not found at {html_path}")

        soup = BeautifulSoup(html_path.read_text(encoding="utf-8"), "html.parser")

        # Placeholder parsing logic – replace with actual selectors
        aqi_value = int(soup.find("span", {"id": "aqi-value"}).text.strip())
        report_datetime_str = soup.find("time", {"id": "report-time"}).attrs["datetime"]
        report_dt = datetime.fromisoformat(report_datetime_str)

        # Validate freshness (e.g., must be within the last 24 hours)
        if datetime.utcnow() - report_dt > timedelta(hours=24):
            raise ValueError("AQI report is older than 24 hours; aborting pipeline.")

        # Duplicate check against PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        sql = "SELECT 1 FROM aqi_facts WHERE report_timestamp = %s LIMIT 1;"
        duplicate = pg_hook.get_first(sql, parameters=(report_dt,))
        skip_load = bool(duplicate)

        if duplicate:
            logging.info("Duplicate record found for %s; skipping load step.", report_dt)

        # Build JSON payload
        payload: Dict[str, Any] = {
            "report_timestamp": report_dt.isoformat(),
            "aqi": aqi_value,
            "source": "Mahidol University",
        }

        output_dir = Path("/tmp/mahidol_aqi")
        json_path = output_dir / "aqi_report.json"
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        logging.info("Extracted data written to %s", json_path)

        ti.xcom_push(key="json_path", value=str(json_path))
        ti.xcom_push(key="skip_load", value=skip_load)

    extract_task = PythonOperator(
        task_id="extract_and_validate_data",
        python_callable=extract_and_validate,
        provide_context=True,
    )

    def load_to_postgres(**context: Any) -> None:
        """
        Load the JSON payload into PostgreSQL tables.
        Skips execution if the duplicate flag is set.
        """
        ti = context["ti"]
        skip_load = ti.xcom_pull(key="skip_load", task_ids="extract_and_validate_data")
        if skip_load:
            logging.info("Skipping load step due to duplicate detection.")
            return

        json_path = Path(ti.xcom_pull(key="json_path", task_ids="extract_and_validate_data"))
        if not json_path.is_file():
            raise FileNotFoundError(f"JSON file not found at {json_path}")

        data = json.loads(json_path.read_text(encoding="utf-8"))
        report_dt = datetime.fromisoformat(data["report_timestamp"])
        aqi = data["aqi"]

        pg_hook = PostgresHook(postgres_conn_id="postgres_default")
        insert_sql = """
            INSERT INTO aqi_facts (report_timestamp, aqi, source)
            VALUES (%s, %s, %s);
        """
        pg_hook.run(insert_sql, parameters=(report_dt, aqi, data["source"]))
        logging.info("Inserted AQI record for %s into PostgreSQL.", report_dt)

    load_task = PythonOperator(
        task_id="load_data_to_postgres",
        python_callable=load_to_postgres,
        provide_context=True,
    )

    def conditional_email_alert(**context: Any) -> None:
        """
        Send an email alert if the AQI exceeds a predefined threshold.
        """
        ti = context["ti"]
        json_path = Path(ti.xcom_pull(key="json_path", task_ids="extract_and_validate_data"))
        if not json_path.is_file():
            raise FileNotFoundError(f"JSON file not found at {json_path}")

        data = json.loads(json_path.read_text(encoding="utf-8"))
        aqi = data["aqi"]
        threshold = 100  # Example threshold; adjust as needed

        if aqi <= threshold:
            logging.info("AQI (%s) is within safe limits; no email sent.", aqi)
            return

        # Retrieve email configuration from an Airflow connection
        conn = BaseHook.get_connection("gmail_smtp")
        smtp_host = conn.host or "smtp.gmail.com"
        smtp_port = conn.port or 587
        smtp_user = conn.login
        smtp_password = conn.password

        # Recipients could be stored in a JSON config; using a placeholder list
        recipients = ["alert@example.com"]

        subject = f"⚠️ AQI Alert: {aqi} (Mahidol University)"
        body = f"""\
Dear recipient,

The latest Air Quality Index (AQI) reported by Mahidol University is {aqi},
which exceeds the safety threshold of {threshold}.

Report time: {data['report_timestamp']}
Source: {data['source']}

Please take appropriate actions.

Best regards,
Air Quality Monitoring System
"""

        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"] = smtp_user
        msg["To"] = ", ".join(recipients)

        try:
            with smtplib.SMTP(smtp_host, smtp_port) as server:
                server.starttls()
                server.login(smtp_user, smtp_password)
                server.sendmail(smtp_user, recipients, msg.as_string())
            logging.info("AQI alert email sent to %s", recipients)
        except Exception as exc:
            logging.error("Failed to send AQI alert email: %s", exc)
            raise

    email_task = PythonOperator(
        task_id="conditional_email_alert",
        python_callable=conditional_email_alert,
        provide_context=True,
    )

    # ------------------------------------------------------------------
    # Set task dependencies (linear flow)
    # ------------------------------------------------------------------
    fetch_html_task >> extract_task >> load_task >> email_task