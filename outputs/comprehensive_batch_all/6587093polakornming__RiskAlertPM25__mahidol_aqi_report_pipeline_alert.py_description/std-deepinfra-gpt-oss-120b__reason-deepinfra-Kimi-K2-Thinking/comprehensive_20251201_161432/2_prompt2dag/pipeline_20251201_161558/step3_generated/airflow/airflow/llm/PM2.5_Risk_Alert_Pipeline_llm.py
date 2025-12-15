# -*- coding: utf-8 -*-
"""
Generated Airflow DAG for PM2.5_Risk_Alert_Pipeline
Author: Automated Generator
Date: 2024-06-28
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd
import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email
from airflow.utils.trigger_rule import TriggerRule

# ----------------------------------------------------------------------
# Default arguments for the DAG
# ----------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ----------------------------------------------------------------------
# DAG definition
# ----------------------------------------------------------------------
with DAG(
    dag_id="PM2.5_Risk_Alert_Pipeline",
    description="Sequential ETL pipeline that scrapes Mahidol University AQI data, "
    "transforms it to JSON, loads it into PostgreSQL, and sends email alerts when "
    "PM2.5 exceeds thresholds.",
    schedule_interval=None,  # Disabled (no schedule)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["aqi", "pm2.5", "etl"],
    max_active_runs=1,
) as dag:

    # ------------------------------------------------------------------
    # Task: extract_mahidol_aqi_html
    # ------------------------------------------------------------------
    @task(task_id="extract_mahidol_aqi_html", retries=0)
    def extract_mahidol_aqi_html() -> str:
        """
        Pulls raw HTML from the Mahidol University AQI website using an HttpHook.
        Returns the raw HTML as a string (stored in XCom).
        """
        http_conn_id = "mahidol_aqi_website_api"
        endpoint = "/"
        hook = HttpHook(http_conn_id=http_conn_id, method="GET")
        try:
            response = hook.run(endpoint)
            response.raise_for_status()
            html = response.text
            logging.info("Successfully fetched AQI HTML (length=%s)", len(html))
            return html
        except Exception as exc:
            logging.error("Failed to fetch AQI HTML: %s", exc)
            raise

    # ------------------------------------------------------------------
    # Task: transform_html_to_json
    # ------------------------------------------------------------------
    @task(task_id="transform_html_to_json", retries=0)
    def transform_html_to_json(raw_html: str) -> List[Dict[str, Any]]:
        """
        Parses the raw HTML and extracts AQI data into a list of dictionaries.
        The structure is ready for loading into PostgreSQL.
        """
        try:
            soup = BeautifulSoup(raw_html, "html.parser")
            # Example parsing logic – adapt to actual page structure
            table = soup.find("table", {"id": "aqi-table"})
            if not table:
                raise ValueError("AQI table not found in HTML.")

            rows = table.find_all("tr")
            data = []
            for row in rows[1:]:  # Skip header
                cols = row.find_all("td")
                if len(cols) < 4:
                    continue
                record = {
                    "station": cols[0].get_text(strip=True),
                    "timestamp": pd.to_datetime(cols[1].get_text(strip=True)),
                    "pm25": float(cols[2].get_text(strip=True)),
                    "pm10": float(cols[3].get_text(strip=True)),
                }
                data.append(record)

            logging.info("Transformed %d rows into JSON.", len(data))
            return data
        except Exception as exc:
            logging.error("Error during HTML to JSON transformation: %s", exc)
            raise

    # ------------------------------------------------------------------
    # Branch: duplicate_check_branch
    # ------------------------------------------------------------------
    def duplicate_check_branch(**context) -> str:
        """
        Checks whether the incoming data already exists in the warehouse.
        If duplicates are found, skips loading; otherwise proceeds to load.
        """
        ti = context["ti"]
        records: List[Dict[str, Any]] = ti.xcom_pull(task_ids="transform_html_to_json")
        if not records:
            raise AirflowSkipException("No records to process after transformation.")

        pg_hook = PostgresHook(postgres_conn_id="postgres_warehouse")
        # Assuming a unique constraint on (station, timestamp)
        duplicate_exists = False
        for rec in records:
            sql = """
                SELECT 1 FROM aqi_measurements
                WHERE station = %s AND timestamp = %s
                LIMIT 1;
            """
            result = pg_hook.get_first(sql, parameters=(rec["station"], rec["timestamp"]))
            if result:
                duplicate_exists = True
                break

        if duplicate_exists:
            logging.info("Duplicate data detected – skipping load.")
            return "skip_load"
        else:
            logging.info("No duplicates found – proceeding to load.")
            return "load_mahidol_aqi_to_warehouse"

    duplicate_check = BranchPythonOperator(
        task_id="duplicate_check_branch",
        python_callable=duplicate_check_branch,
        provide_context=True,
        retries=0,
    )

    # ------------------------------------------------------------------
    # Task: load_mahidol_aqi_to_warehouse
    # ------------------------------------------------------------------
    @task(task_id="load_mahidol_aqi_to_warehouse", retries=0)
    def load_mahidol_aqi_to_warehouse(records: List[Dict[str, Any]]) -> None:
        """
        Inserts the transformed AQI records into the PostgreSQL warehouse.
        """
        pg_hook = PostgresHook(postgres_conn_id="postgres_warehouse")
        insert_sql = """
            INSERT INTO aqi_measurements (station, timestamp, pm25, pm10)
            VALUES (%s, %s, %s, %s);
        """
        try:
            for rec in records:
                pg_hook.run(
                    insert_sql,
                    parameters=(
                        rec["station"],
                        rec["timestamp"],
                        rec["pm25"],
                        rec["pm10"],
                    ),
                )
            logging.info("Successfully loaded %d records into the warehouse.", len(records))
        except Exception as exc:
            logging.error("Failed to load records into PostgreSQL: %s", exc)
            raise

    # ------------------------------------------------------------------
    # Branch: aqi_threshold_branch
    # ------------------------------------------------------------------
    def aqi_threshold_branch(**context) -> str:
        """
        Determines if any PM2.5 value exceeds the configured threshold.
        If so, triggers the email alert; otherwise skips notification.
        """
        threshold = float(Variable.get("pm25_alert_threshold", default_var=35.0))
        ti = context["ti"]
        records: List[Dict[str, Any]] = ti.xcom_pull(task_ids="transform_html_to_json")
        if not records:
            raise AirflowSkipException("No records available for threshold evaluation.")

        exceeders = [r for r in records if r["pm25"] > threshold]
        if exceeders:
            logging.info(
                "PM2.5 threshold exceeded in %d stations. Triggering alert.", len(exceeders)
            )
            # Store exceeders for the email task
            ti.xcom_push(key="exceeders", value=exceeders)
            return "notify_pm25_alert"
        else:
            logging.info("No PM2.5 values exceed the threshold of %s µg/m³.", threshold)
            return "skip_notification"

    aqi_threshold = BranchPythonOperator(
        task_id="aqi_threshold_branch",
        python_callable=aqi_threshold_branch,
        provide_context=True,
        retries=0,
    )

    # ------------------------------------------------------------------
    # Task: notify_pm25_alert
    # ------------------------------------------------------------------
    @task(task_id="notify_pm25_alert", retries=0)
    def notify_pm25_alert() -> None:
        """
        Sends an email alert containing stations where PM2.5 exceeds the threshold.
        """
        ti = notify_pm25_alert.get_current_context()["ti"]
        exceeders: List[Dict[str, Any]] = ti.xcom_pull(key="exceeders", task_ids="aqi_threshold_branch")
        if not exceeders:
            logging.warning("No exceeders found in XCom – skipping email.")
            return

        # Build email content
        subject = "⚠️ PM2.5 Alert – Threshold Exceeded"
        html_content = "<h3>PM2.5 levels have exceeded the configured threshold:</h3><ul>"
        for rec in exceeders:
            html_content += (
                f"<li>{rec['station']} at {rec['timestamp']} – "
                f"PM2.5: {rec['pm25']} µg/m³</li>"
            )
        html_content += "</ul>"

        # Retrieve email configuration from Airflow Variables or Connections
        recipient = Variable.get("pm25_alert_recipient", default_var="alerts@example.com")
        sender = Variable.get("pm25_alert_sender", default_var="airflow@example.com")

        try:
            send_email(to=recipient, subject=subject, html_content=html_content, from_email=sender)
            logging.info("PM2.5 alert email sent to %s.", recipient)
        except Exception as exc:
            logging.error("Failed to send PM2.5 alert email: %s", exc)
            raise

    # ------------------------------------------------------------------
    # Dummy tasks for skipped branches (to keep DAG tidy)
    # ------------------------------------------------------------------
    skip_load = PythonOperator(
        task_id="skip_load",
        python_callable=lambda: logging.info("Load step skipped due to duplicate data."),
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    skip_notification = PythonOperator(
        task_id="skip_notification",
        python_callable=lambda: logging.info("Notification step skipped – no threshold breach."),
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # ------------------------------------------------------------------
    # Define task pipeline
    # ------------------------------------------------------------------
    raw_html = extract_mahidol_aqi_html()
    json_records = transform_html_to_json(raw_html)

    # Branching for duplicate check
    duplicate_check >> [load_mahidol_aqi_to_warehouse(json_records), skip_load]

    # Branching for threshold alert
    aqi_threshold >> [notify_pm25_alert(), skip_notification]

    # Set overall ordering
    raw_html >> json_records
    json_records >> duplicate_check
    json_records >> aqi_threshold

# End of DAG definition.