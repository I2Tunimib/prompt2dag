import logging
import os
from datetime import datetime, timedelta

import requests
from airflow import DAG, Variable
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=60),
    "start_date": datetime(2024, 1, 1),
}

# Retrieve schedule interval from Airflow Variable
schedule_interval = Variable.get("pcd_etl_schedule", default_var=None)

# Email configuration variables
email_recipients = Variable.get("pcd_etl_email_recipients", default_var="data-team@example.com")
email_subject_success = Variable.get(
    "pcd_etl_email_subject_success", default_var="PCD ETL Pipeline Succeeded"
)
email_subject_failure = Variable.get(
    "pcd_etl_email_subject_failure", default_var="PCD ETL Pipeline Failed"
)


def fetch_api(endpoint_var_name: str, **context):
    """
    Generic function to fetch data from an HTTP API endpoint.
    The endpoint URL is stored in an Airflow Variable with the given name.
    """
    url = Variable.get(endpoint_var_name, default_var=None)
    if not url:
        raise AirflowFailException(f"Variable '{endpoint_var_name}' is not set.")
    logging.info("Fetching data from %s", url)
    try:
        response = requests.get(url, timeout=300)
        response.raise_for_status()
        # In a real pipeline, you would store the response or process it further.
        logging.info("Successfully fetched data from %s (status %s)", url, response.status_code)
    except Exception as exc:
        logging.error("Error fetching data from %s: %s", url, exc)
        raise


def send_etl_notification(**context):
    """
    Sends an email notification indicating the overall DAG run status.
    This task runs with trigger_rule='all_done' to ensure execution regardless of upstream failures.
    """
    dag_run = context.get("dag_run")
    if not dag_run:
        logging.warning("No dag_run object available in context.")
        return

    state = dag_run.get_state()
    subject = email_subject_success if state == "success" else email_subject_failure
    body = f"PCD ETL pipeline completed with state: {state}\n\n"
    body += "Task details:\n"
    for ti in dag_run.get_task_instances():
        body += f"- {ti.task_id}: {ti.state}\n"

    try:
        send_email(to=email_recipients.split(","), subject=subject, html_content=body)
        logging.info("Notification email sent to %s", email_recipients)
    except Exception as exc:
        logging.error("Failed to send notification email: %s", exc)
        raise


with DAG(
    dag_id="pcd_etl_pipeline",
    default_args=default_args,
    schedule_interval=schedule_interval,
    catchup=False,
    tags=["pcd", "etl"],
    description="ETL pipeline for Primary Care Data (PCD)",
) as dag:

    # Kubernetes job to verify files in SFTP folder
    check_pcd_sftp_folder = KubernetesPodOperator(
        task_id="Check_PCD_SFTP_Folder",
        name="check-pcd-sftp-folder",
        namespace="default",
        image="alpine:3.18",
        cmds=["sh", "-c"],
        arguments=["echo 'Checking SFTP folder...'; sleep 10"],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # Kubernetes job to verify files in shared folder
    check_pcd_shared_folder = KubernetesPodOperator(
        task_id="Check_PCD_Shared_Folder",
        name="check-pcd-shared-folder",
        namespace="default",
        image="alpine:3.18",
        cmds=["sh", "-c"],
        arguments=["echo 'Checking shared folder...'; sleep 10"],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # Synchronization point before parallel extraction
    start_pcd_extract_1 = EmptyOperator(task_id="Start_PCD_Extract_1")

    # List of API extraction tasks
    api_tasks = [
        ("Status_Tracker", "status_tracker_endpoint"),
        ("Financial_Expense", "financial_expense_endpoint"),
        ("UPCC_Financial_Reportingr", "upcc_financial_reporting_endpoint"),
        ("CHC_Financial_reporting", "chc_financial_reporting_endpoint"),
        ("PCN_Financial_Reporting", "pcn_financial_reporting_endpoint"),
        ("NPPCC_Financial_Reporting", "nppcc_financial_reporting_endpoint"),
        ("Fiscal_Year_Reporting_Dates", "fiscal_year_reporting_dates_endpoint"),
        ("UPCC_Primary_Care_Patient_Services", "upcc_primary_care_patient_services_endpoint"),
        ("CHC_Primary_Care_Patient_Services", "chc_primary_care_patient_services_endpoint"),
        ("Practitioner_Role_Mapping", "practitioner_role_mapping_endpoint"),
        ("HR_Records", "hr_records_endpoint"),
        ("Provincial_Risk_Tracking", "provincial_risk_tracking_endpoint"),
        ("Decision_Log", "decision_log_endpoint"),
        ("HA_Hierarchy", "ha_hierarchy_endpoint"),
        ("UPPC_Budget", "uppc_budget_endpoint"),
        ("CHC_Budget", "chc_budget_endpoint"),
        ("PCN_Budget", "pcn_budget_endpoint"),
        ("NPPCC_Budget", "nppcc_budget_endpoint"),
    ]

    extraction_operators = []
    for task_name, var_name in api_tasks:
        op = PythonOperator(
            task_id=task_name,
            python_callable=fetch_api,
            op_kwargs={"endpoint_var_name": var_name},
        )
        extraction_operators.append(op)

    # Additional synchronization after Status_Tracker
    start_pcd_extract_2 = EmptyOperator(task_id="Start_PCD_Extract_2")

    # Kubernetes job to perform the main ETL file upload
    pcd_file_upload = KubernetesPodOperator(
        task_id="PCD_file_upload",
        name="pcd-file-upload",
        namespace="default",
        image="alpine:3.18",
        cmds=["sh", "-c"],
        arguments=["echo 'Uploading ETL file...'; sleep 20"],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # Notification task that runs regardless of upstream success/failure
    etl_notification = PythonOperator(
        task_id="ETL_Notification",
        python_callable=send_etl_notification,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Define dependencies
    check_pcd_sftp_folder >> check_pcd_shared_folder >> start_pcd_extract_1

    start_pcd_extract_1 >> extraction_operators

    # Status_Tracker is the first element in api_tasks list
    extraction_operators[0] >> start_pcd_extract_2

    # All extraction tasks (including the sync point) must complete before upload
    extraction_operators >> pcd_file_upload
    start_pcd_extract_2 >> pcd_file_upload

    pcd_file_upload >> etl_notification