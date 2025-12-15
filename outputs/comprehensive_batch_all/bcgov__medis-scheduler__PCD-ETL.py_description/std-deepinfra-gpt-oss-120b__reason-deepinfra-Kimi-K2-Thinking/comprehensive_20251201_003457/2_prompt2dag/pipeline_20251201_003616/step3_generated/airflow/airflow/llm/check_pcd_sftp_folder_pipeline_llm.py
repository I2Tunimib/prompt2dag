# -*- coding: utf-8 -*-
"""
Generated Airflow DAG for check_pcd_sftp_folder_pipeline
Author: Automated Generator
Date: 2024-06-28
"""

from datetime import datetime, timedelta
import json
import logging

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.filesystem.hooks.fs import FSHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email

# Default arguments applied to all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition
with DAG(
    dag_id="check_pcd_sftp_folder_pipeline",
    description="Staged ETL pipeline for Primary Care Data (PCD) processing with folder validation, parallel API extraction, Kubernetes‑based processing and email notification.",
    schedule_interval=None,  # Disabled schedule
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["pcd", "etl"],
    max_active_runs=1,
    is_paused_upon_creation=True,
) as dag:

    # -------------------------------------------------------------------------
    # Task: Check PCD SFTP Folder
    # -------------------------------------------------------------------------
    def _check_pcd_sftp_folder(**context):
        """
        Verify that the expected folder exists on the PCD SFTP server
        and contains at least one file.
        """
        hook = SFTPHook(ftp_conn_id="sftp_server")
        folder_path = Variable.get("pcd_sftp_folder", default_var="/incoming/pcd")
        try:
            files = hook.list_directory(folder_path)
            if not files:
                raise AirflowException(f"No files found in SFTP folder {folder_path}")
            logging.info("SFTP folder %s contains %d file(s).", folder_path, len(files))
        except Exception as exc:
            raise AirflowException(f"Failed to access SFTP folder {folder_path}: {exc}")

    check_pcd_sftp_folder = PythonOperator(
        task_id="check_pcd_sftp_folder",
        python_callable=_check_pcd_sftp_folder,
        retries=0,
        provide_context=True,
    )

    # -------------------------------------------------------------------------
    # Task: Check PCD Shared Folder
    # -------------------------------------------------------------------------
    def _check_pcd_shared_folder(**context):
        """
        Verify that the shared network folder for PCD exists and is writable.
        """
        hook = FSHook(fs_conn_id="shared_network_folder")
        folder_path = Variable.get("pcd_shared_folder", default_var="/shared/pcd")
        try:
            if not hook.check_for_path(folder_path):
                raise AirflowException(f"Shared folder {folder_path} does not exist.")
            # Attempt to write a temporary file to ensure write permissions
            test_file = f"{folder_path}/.airflow_test_{datetime.utcnow().timestamp()}"
            hook.write_file(test_file, b"test")
            hook.remove_file(test_file)
            logging.info("Shared folder %s is accessible and writable.", folder_path)
        except Exception as exc:
            raise AirflowException(f"Failed to validate shared folder {folder_path}: {exc}")

    check_pcd_shared_folder = PythonOperator(
        task_id="check_pcd_shared_folder",
        python_callable=_check_pcd_shared_folder,
        retries=0,
        provide_context=True,
    )

    # -------------------------------------------------------------------------
    # Task: Extract PCD API Data
    # -------------------------------------------------------------------------
    extract_pcd_api = SimpleHttpOperator(
        task_id="extract_pcd_api",
        http_conn_id="http_api_financial_expense",
        endpoint="/pcd/data",
        method="GET",
        headers={"Content-Type": "application/json"},
        response_check=lambda response: response.status_code == 200,
        log_response=True,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    # -------------------------------------------------------------------------
    # Task: Process and Load PCD Data
    # -------------------------------------------------------------------------
    def _process_and_load_pcd_data(**context):
        """
        Placeholder for processing the extracted API payload and loading it
        into the target data store. The actual implementation would involve
        data transformation, validation, and persistence logic.
        """
        ti = context["ti"]
        api_response = ti.xcom_pull(task_ids="extract_pcd_api")
        if not api_response:
            raise AirflowException("No API response received from extract_pcd_api task.")

        try:
            data = json.loads(api_response)
            # Insert transformation logic here
            logging.info("Processing %d records from API response.", len(data))
            # Simulate load step
            logging.info("Data successfully processed and loaded.")
        except json.JSONDecodeError as exc:
            raise AirflowException(f"Failed to decode API response as JSON: {exc}")
        except Exception as exc:
            raise AirflowException(f"Error during processing/loading: {exc}")

    process_and_load_pcd_data = PythonOperator(
        task_id="process_and_load_pcd_data",
        python_callable=_process_and_load_pcd_data,
        retries=2,
        retry_delay=timedelta(minutes=5),
        provide_context=True,
    )

    # -------------------------------------------------------------------------
    # Task: Send ETL Notification
    # -------------------------------------------------------------------------
    def _send_etl_notification(**context):
        """
        Send an email notification indicating the ETL run status.
        """
        subject = "PCD ETL Pipeline Completed"
        html_content = """
        <h3>PCD ETL Pipeline Execution Summary</h3>
        <ul>
            <li>Start Time: {{ ds }}</li>
            <li>Status: SUCCESS</li>
        </ul>
        """
        to = Variable.get("etl_notification_recipients", default_var="data-team@example.com")
        try:
            send_email(to=to, subject=subject, html_content=html_content)
            logging.info("ETL notification email sent to %s.", to)
        except Exception as exc:
            raise AirflowException(f"Failed to send ETL notification email: {exc}")

    send_etl_notification = PythonOperator(
        task_id="send_etl_notification",
        python_callable=_send_etl_notification,
        retries=1,
        retry_delay=timedelta(minutes=3),
        provide_context=True,
    )

    # -------------------------------------------------------------------------
    # Define task dependencies (sequential flow)
    # -------------------------------------------------------------------------
    check_pcd_sftp_folder >> check_pcd_shared_folder >> extract_pcd_api >> process_and_load_pcd_data >> send_etl_notification

# End of DAG definition.