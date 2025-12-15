from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

# Default arguments for the DAG
default_args = {
    'owner': 'pcd-data-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

# Fetch configuration from Airflow Variables
SCHEDULE_INTERVAL = Variable.get("pcd_etl_schedule", default_var=None)
EMAIL_RECIPIENTS = Variable.get("pcd_email_recipients", default_var="").split(",")
K8S_NAMESPACE = Variable.get("pcd_k8s_namespace", default_var="default")
K8S_IMAGE = Variable.get("pcd_k8s_image", default_var="busybox:latest")

# Stub functions for API extraction tasks
def extract_status_tracker(**context):
    """Extract data from Status Tracker API endpoint."""
    logging.info("Extracting Status Tracker data")
    # Implementation would go here
    return "Status_Tracker extraction completed"

def extract_financial_expense(**context):
    """Extract data from Financial Expense API endpoint."""
    logging.info("Extracting Financial Expense data")
    return "Financial_Expense extraction completed"

def extract_upcc_financial_reporting(**context):
    """Extract data from UPCC Financial Reporting API endpoint."""
    logging.info("Extracting UPCC Financial Reporting data")
    return "UPCC_Financial_Reportingr extraction completed"

def extract_chc_financial_reporting(**context):
    """Extract data from CHC Financial Reporting API endpoint."""
    logging.info("Extracting CHC Financial Reporting data")
    return "CHC_Financial_reporting extraction completed"

def extract_pcn_financial_reporting(**context):
    """Extract data from PCN Financial Reporting API endpoint."""
    logging.info("Extracting PCN Financial Reporting data")
    return "PCN_Financial_Reporting extraction completed"

def extract_nppcc_financial_reporting(**context):
    """Extract data from NPPCC Financial Reporting API endpoint."""
    logging.info("Extracting NPPCC Financial Reporting data")
    return "NPPCC_Financial_Reporting extraction completed"

def extract_fiscal_year_reporting_dates(**context):
    """Extract Fiscal Year Reporting Dates."""
    logging.info("Extracting Fiscal Year Reporting Dates")
    return "Fiscal_Year_Reporting_Dates extraction completed"

def extract_upcc_primary_care_patient_services(**context):
    """Extract UPCC Primary Care Patient Services data."""
    logging.info("Extracting UPCC Primary Care Patient Services data")
    return "UPCC_Primary_Care_Patient_Services extraction completed"

def extract_chc_primary_care_patient_services(**context):
    """Extract CHC Primary Care Patient Services data."""
    logging.info("Extracting CHC Primary Care Patient Services data")
    return "CHC_Primary_Care_Patient_Services extraction completed"

def extract_practitioner_role_mapping(**context):
    """Extract Practitioner Role Mapping data."""
    logging.info("Extracting Practitioner Role Mapping data")
    return "Practitioner_Role_Mapping extraction completed"

def extract_hr_records(**context):
    """Extract HR Records data."""
    logging.info("Extracting HR Records data")
    return "HR_Records extraction completed"

def extract_provincial_risk_tracking(**context):
    """Extract Provincial Risk Tracking data."""
    logging.info("Extracting Provincial Risk Tracking data")
    return "Provincial_Risk_Tracking extraction completed"

def extract_decision_log(**context):
    """Extract Decision Log data."""
    logging.info("Extracting Decision Log data")
    return "Decision_Log extraction completed"

def extract_ha_hierarchy(**context):
    """Extract HA Hierarchy data."""
    logging.info("Extracting HA Hierarchy data")
    return "HA_Hierarchy extraction completed"

def extract_uppc_budget(**context):
    """Extract UPPC Budget data."""
    logging.info("Extracting UPPC Budget data")
    return "UPPC_Budget extraction completed"

def extract_chc_budget(**context):
    """Extract CHC Budget data."""
    logging.info("Extracting CHC Budget data")
    return "CHC_Budget extraction completed"

def extract_pcn_budget(**context):
    """Extract PCN Budget data."""
    logging.info("Extracting PCN Budget data")
    return "PCN_Budget extraction completed"

def extract_nppcc_budget(**context):
    """Extract NPPCC Budget data."""
    logging.info("Extracting NPPCC Budget data")
    return "NPPCC_Budget extraction completed"

def send_etl_notification(**context):
    """Send email notification based on DAG run status."""
    from airflow.utils.email import send_email
    
    dag_run = context['dag_run']
    task_instances = dag_run.get_task_instances()
    
    failed_tasks = [ti for ti in task_instances if ti.state == 'failed']
    
    if failed_tasks:
        subject = f"PCD ETL Pipeline Failed - {dag_run.execution_date}"
        body = f"""
        <h3>PCD ETL Pipeline Execution Failed</h3>
        <p><strong>Execution Date:</strong> {dag_run.execution_date}</p>
        <p><strong>DAG Run ID:</strong> {dag_run.run_id}</p>
        <p><strong>Failed Tasks:</strong></p>
        <ul>
        {''.join(f'<li>{ti.task_id}</li>' for ti in failed_tasks)}
        </ul>
        <p>Please check the Airflow UI for detailed logs.</p>
        """
    else:
        subject = f"PCD ETL Pipeline Succeeded - {dag_run.execution_date}"
        body = f"""
        <h3>PCD ETL Pipeline Execution Successful</h3>
        <p><strong>Execution Date:</strong> {dag_run.execution_date}</p>
        <p><strong>DAG Run ID:</strong> {dag_run.run_id}</p>
        <p>All tasks completed successfully.</p>
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
    dagrun_timeout=timedelta(minutes=60),
    tags=['pcd', 'etl', 'kubernetes'],
    max_active_runs=1,
) as dag:

    # Kubernetes folder check tasks
    check_pcd_sftp_folder = KubernetesPodOperator(
        task_id='check_pcd_sftp_folder',
        name='check-pcd-sftp-folder',
        namespace=K8S_NAMESPACE,
        image=K8S_IMAGE,
        cmds=['sh', '-c'],
        arguments=['echo "Checking SFTP folder"; sleep 5; echo "SFTP check completed"'],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    check_pcd_shared_folder = KubernetesPodOperator(
        task_id='check_pcd_shared_folder',
        name='check-pcd-shared-folder',
        namespace=K8S_NAMESPACE,
        image=K8S_IMAGE,
        cmds=['sh', '-c'],
        arguments=['echo "Checking shared folder"; sleep 5; echo "Shared folder check completed"'],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # Synchronization points
    start_pcd_extract_1 = EmptyOperator(task_id='start_pcd_extract_1')
    start_pcd_extract_2 = EmptyOperator(task_id='start_pcd_extract_2')

    # Parallel HTTP API extraction tasks
    extract_tasks = {
        'status_tracker': PythonOperator(
            task_id='status_tracker',
            python_callable=extract_status_tracker,
        ),
        'financial_expense': PythonOperator(
            task_id='financial_expense',
            python_callable=extract_financial_expense,
        ),
        'upcc_financial_reporting': PythonOperator(
            task_id='upcc_financial_reporting',
            python_callable=extract_upcc_financial_reporting,
        ),
        'chc_financial_reporting': PythonOperator(
            task_id='chc_financial_reporting',
            python_callable=extract_chc_financial_reporting,
        ),
        'pcn_financial_reporting': PythonOperator(
            task_id='pcn_financial_reporting',
            python_callable=extract_pcn_financial_reporting,
        ),
        'nppcc_financial_reporting': PythonOperator(
            task_id='nppcc_financial_reporting',
            python_callable=extract_nppcc_financial_reporting,
        ),
        'fiscal_year_reporting_dates': PythonOperator(
            task_id='fiscal_year_reporting_dates',
            python_callable=extract_fiscal_year_reporting_dates,
        ),
        'upcc_primary_care_patient_services': PythonOperator(
            task_id='upcc_primary_care_patient_services',
            python_callable=extract_upcc_primary_care_patient_services,
        ),
        'chc_primary_care_patient_services': PythonOperator(
            task_id='chc_primary_care_patient_services',
            python_callable=extract_chc_primary_care_patient_services,
        ),
        'practitioner_role_mapping': PythonOperator(
            task_id='practitioner_role_mapping',
            python_callable=extract_practitioner_role_mapping,
        ),
        'hr_records': PythonOperator(
            task_id='hr_records',
            python_callable=extract_hr_records,
        ),
        'provincial_risk_tracking': PythonOperator(
            task_id='provincial_risk_tracking',
            python_callable=extract_provincial_risk_tracking,
        ),
        'decision_log': PythonOperator(
            task_id='decision_log',
            python_callable=extract_decision_log,
        ),
        'ha_hierarchy': PythonOperator(
            task_id='ha_hierarchy',
            python_callable=extract_ha_hierarchy,
        ),
        'uppc_budget': PythonOperator(
            task_id='uppc_budget',
            python_callable=extract_uppc_budget,
        ),
        'chc_budget': PythonOperator(
            task_id='chc_budget',
            python_callable=extract_chc_budget,
        ),
        'pcn_budget': PythonOperator(
            task_id='pcn_budget',
            python_callable=extract_pcn_budget,
        ),
        'nppcc_budget': PythonOperator(
            task_id='nppcc_budget',
            python_callable=extract_nppcc_budget,
        ),
    }

    # Kubernetes file upload task
    pcd_file_upload = KubernetesPodOperator(
        task_id='pcd_file_upload',
        name='pcd-file-upload',
        namespace=K8S_NAMESPACE,
        image=K8S_IMAGE,
        cmds=['sh', '-c'],
        arguments=['echo "Performing PCD file upload"; sleep 10; echo "Upload completed"'],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # Notification task with all_done trigger rule
    etl_notification = PythonOperator(
        task_id='etl_notification',
        python_callable=send_etl_notification,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Task dependencies
    check_pcd_sftp_folder >> check_pcd_shared_folder >> start_pcd_extract_1
    
    # All extraction tasks triggered by start_pcd_extract_1
    for task in extract_tasks.values():
        start_pcd_extract_1 >> task
    
    # Status_Tracker triggers start_pcd_extract_2
    extract_tasks['status_tracker'] >> start_pcd_extract_2
    
    # All extraction tasks must complete before upload
    list(extract_tasks.values()) >> pcd_file_upload
    
    # Upload task triggers notification (which runs regardless of success/failure)
    pcd_file_upload >> etl_notification