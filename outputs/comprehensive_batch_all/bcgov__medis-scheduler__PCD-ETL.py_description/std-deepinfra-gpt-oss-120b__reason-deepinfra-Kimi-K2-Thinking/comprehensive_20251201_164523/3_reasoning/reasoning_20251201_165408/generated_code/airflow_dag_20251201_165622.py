from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.models import Variable
from airflow.utils.email import send_email
from airflow.utils.trigger_rule import TriggerRule

# Default arguments
default_args = {
    'owner': 'pcd-etl-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=60),
}

# Configuration from Airflow Variables
SCHEDULE_INTERVAL = Variable.get("pcd_etl_schedule", default_var=None)
K8S_NAMESPACE = Variable.get("pcd_k8s_namespace", default_var="default")
K8S_IMAGE = Variable.get("pcd_k8s_image", default_var="python:3.9-slim")
EMAIL_RECIPIENTS = Variable.get("pcd_email_recipients", default_var="").split(",")

# Placeholder functions for HTTP API extractions
def extract_status_tracker(**context):
    """Extract data from Status Tracker API endpoint."""
    # Implementation would call the actual HTTP API
    pass

def extract_financial_expense(**context):
    """Extract data from Financial Expense API endpoint."""
    pass

# ... and so on for all 18 extraction functions

def send_etl_notification(**context):
    """Send email notification based on DAG run status."""
    dag_run = context['dag_run']
    task_instances = dag_run.get_task_instances()
    
    failed_tasks = [ti for ti in task_instances if ti.state == 'failed']
    
    subject = f"PCD ETL Pipeline {'Failed' if failed_tasks else 'Succeeded'} - Run {dag_run.run_id}"
    body = f"""
    <h3>PCD ETL Pipeline Execution Report</h3>
    <p><strong>Run ID:</strong> {dag_run.run_id}</p>
    <p><strong>Execution Date:</strong> {context['execution_date']}</p>
    <p><strong>Status:</strong> {'<span style="color:red">FAILED</span>' if failed_tasks else '<span style="color:green">SUCCESS</span>'}</p>
    """
    
    if failed_tasks:
        body += f"""
        <h4>Failed Tasks:</h4>
        <ul>
        {''.join(f'<li>{ti.task_id}</li>' for ti in failed_tasks)}
        </ul>
        """
    
    send_email(
        to=EMAIL_RECIPIENTS,
        subject=subject,
        html_content=body
    )

# DAG definition
with DAG(
    dag_id='pcd_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Primary Care Data extraction and processing',
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['pcd', 'etl', 'kubernetes'],
) as dag:
    
    # Folder check tasks
    check_sftp_folder = KubernetesPodOperator(
        task_id='check_pcd_sftp_folder',
        namespace=K8S_NAMESPACE,
        image=K8S_IMAGE,
        cmds=["python", "-c"],
        arguments=["print('Checking SFTP folder')"],  # Placeholder
        name="check-sftp-folder",
        get_logs=True,
    )
    
    check_shared_folder = KubernetesPodOperator(
        task_id='check_pcd_shared_folder',
        namespace=K8S_NAMESPACE,
        image=K8S_IMAGE,
        cmds=["python", "-c"],
        arguments=["print('Checking shared folder')"],  # Placeholder
        name="check-shared-folder",
        get_logs=True,
    )
    
    # Synchronization points
    start_extract_1 = EmptyOperator(task_id='start_pcd_extract_1')
    start_extract_2 = EmptyOperator(task_id='start_pcd_extract_2')
    
    # Parallel HTTP API extraction tasks
    extract_tasks = []
    
    status_tracker = PythonOperator(
        task_id='status_tracker',
        python_callable=extract_status_tracker,
    )
    extract_tasks.append(status_tracker)
    
    financial_expense = PythonOperator(
        task_id='financial_expense',
        python_callable=extract_financial_expense,
    )
    extract_tasks.append(financial_expense)
    
    # ... continue for all 18 tasks
    
    # File upload task
    pcd_file_upload = KubernetesPodOperator(
        task_id='pcd_file_upload',
        namespace=K8S_NAMESPACE,
        image=K8S_IMAGE,
        cmds=["python", "-c"],
        arguments=["print('Performing ETL file upload')"],  # Placeholder
        name="pcd-file-upload",
        get_logs=True,
    )
    
    # Notification task
    etl_notification = PythonOperator(
        task_id='etl_notification',
        python_callable=send_etl_notification,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    
    # Define dependencies
    check_sftp_folder >> check_shared_folder >> start_extract_1
    
    start_extract_1 >> extract_tasks
    
    status_tracker >> start_extract_2
    
    # PCD_file_upload depends on all extraction tasks and start_extract_2
    extract_tasks + [start_extract_2] >> pcd_file_upload
    
    pcd_file_upload >> etl_notification