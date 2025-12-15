import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
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
    "email_on_failure": False,
    "email_on_retry": False,
}

# Retrieve schedule from Airflow Variable; default to None (manual trigger)
schedule = Variable.get("pcd_etl_schedule", default_var=None)

# DAG definition
with DAG(
    dag_id="pcd_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for Primary Care Data (PCD)",
    schedule_interval=schedule,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["pcd", "etl"],
) as dag:

    # Helper function to run generic API extraction
    def run_api_extraction(task_name: str, **context):
        """Placeholder for API extraction logic."""
        logging.info("Running API extraction for %s", task_name)
        # Here you would implement the actual HTTP request using
        # variables such as endpoint URLs, authentication, etc.
        # For demonstration, we just log the action.

    # Helper function for notification email
    def send_etl_notification(**context):
        """Send success or failure email based on DAG run state."""
        dag_run = context.get("dag_run")
        if not dag_run:
            logging.warning("No dag_run context available.")
            return

        success = dag_run.get_state() == "success"
        subject = (
            "PCD ETL Pipeline Succeeded"
            if success
            else "PCD ETL Pipeline Failed"
        )
        body = f"The PCD ETL pipeline has {'succeeded' if success else 'failed'}.\n"
        body += f"DAG Run: {dag_run.run_id}\n"
        body += f"Execution Date: {dag_run.execution_date}\n"

        recipients = Variable.get(
            "pcd_etl_email_recipients", default_var=""
        ).split(",")
        if recipients:
            send_email(to=recipients, subject=subject, html_content=body)
            logging.info("Notification email sent to %s", recipients)
        else:
            logging.warning("No email recipients defined for notification.")

    # Kubernetes job to check SFTP folder
    check_pcd_sftp_folder = KubernetesPodOperator(
        task_id="Check_PCD_SFTP_Folder",
        name="check-pcd-sftp-folder",
        namespace=Variable.get("k8s_namespace", default_var="default"),
        image=Variable.get(
            "check_sftp_image", default_var="alpine:3.14"
        ),
        cmds=["sh", "-c"],
        arguments=[
            Variable.get(
                "check_sftp_command",
                default_var="echo 'Checking SFTP folder...'",
            )
        ],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # Kubernetes job to check shared folder
    check_pcd_shared_folder = KubernetesPodOperator(
        task_id="Check_PCD_Shared_Folder",
        name="check-pcd-shared-folder",
        namespace=Variable.get("k8s_namespace", default_var="default"),
        image=Variable.get(
            "check_shared_image", default_var="alpine:3.14"
        ),
        cmds=["sh", "-c"],
        arguments=[
            Variable.get(
                "check_shared_command",
                default_var="echo 'Checking shared folder...'",
            )
        ],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # Synchronization point before parallel extraction
    start_pcd_extract_1 = EmptyOperator(task_id="Start_PCD_Extract_1")

    # List of parallel extraction task names
    extraction_tasks_names = [
        "Status_Tracker",
        "Financial_Expense",
        "UPCC_Financial_Reportingr",
        "CHC_Financial_reporting",
        "PCN_Financial_Reporting",
        "NPPCC_Financial_Reporting",
        "Fiscal_Year_Reporting_Dates",
        "UPCC_Primary_Care_Patient_Services",
        "CHC_Primary_Care_Patient_Services",
        "Practitioner_Role_Mapping",
        "HR_Records",
        "Provincial_Risk_Tracking",
        "Decision_Log",
        "HA_Hierarchy",
        "UPPC_Budget",
        "CHC_Budget",
        "PCN_Budget",
        "NPPCC_Budget",
    ]

    # Create PythonOperator tasks for each extraction
    extraction_tasks = []
    for name in extraction_tasks_names:
        task = PythonOperator(
            task_id=name,
            python_callable=run_api_extraction,
            op_kwargs={"task_name": name},
        )
        extraction_tasks.append(task)

    # Additional synchronization after Status_Tracker
    start_pcd_extract_2 = EmptyOperator(task_id="Start_PCD_Extract_2")

    # Kubernetes job for the main file upload
    pcd_file_upload = KubernetesPodOperator(
        task_id="PCD_file_upload",
        name="pcd-file-upload",
        namespace=Variable.get("k8s_namespace", default_var="default"),
        image=Variable.get(
            "file_upload_image", default_var="alpine:3.14"
        ),
        cmds=["sh", "-c"],
        arguments=[
            Variable.get(
                "file_upload_command",
                default_var="echo 'Uploading PCD files...'",
            )
        ],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # Notification task that runs regardless of upstream success/failure
    etl_notification = PythonOperator(
        task_id="ETL_Notification",
        python_callable=send_etl_notification,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Define task dependencies
    check_pcd_sftp_folder >> check_pcd_shared_folder >> start_pcd_extract_1

    start_pcd_extract_1 >> extraction_tasks

    # Status_Tracker is the first task in extraction_tasks list
    status_tracker_task = dag.get_task("Status_Tracker")
    status_tracker_task >> start_pcd_extract_2

    # All extraction tasks (including start_pcd_extract_2) must complete before upload
    extraction_tasks >> start_pcd_extract_2
    start_pcd_extract_2 >> pcd_file_upload

    # Upload depends on all extraction tasks as well
    for task in extraction_tasks:
        task >> pcd_file_upload

    pcd_file_upload >> etl_notification