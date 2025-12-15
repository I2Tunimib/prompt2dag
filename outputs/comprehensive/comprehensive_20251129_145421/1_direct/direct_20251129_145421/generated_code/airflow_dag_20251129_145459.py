from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_job import KubernetesJobOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from datetime import timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'dagrun_timeout': timedelta(minutes=60),
}

def send_etl_notification(**kwargs):
    ti = kwargs['ti']
    dag_run = kwargs['dag_run']
    status = 'Success' if dag_run.state == 'success' else 'Failure'
    failed_tasks = [task for task in dag_run.get_task_instances() if task.state == 'failed']
    failed_task_ids = [task.task_id for task in failed_tasks]
    
    subject = f"PCD ETL Pipeline {status} - {dag_run.execution_date}"
    html_content = f"""
    <h3>PCD ETL Pipeline {status} - {dag_run.execution_date}</h3>
    <p>Environment: {Variable.get('Environment')}</p>
    <p>Airflow URL: {Variable.get('airflow_url')}</p>
    <p>Failed Tasks: {', '.join(failed_task_ids)}</p>
    """
    
    if status == 'Success':
        recipients = Variable.get('PCD_ETL_email_list_success')
    else:
        recipients = Variable.get('ETL_email_list_alerts')
    
    ti.xcom_push(key='email_subject', value=subject)
    ti.xcom_push(key='email_html_content', value=html_content)
    ti.xcom_push(key='email_recipients', value=recipients)

with DAG(
    'pcd_etl_pipeline',
    default_args=default_args,
    description='Comprehensive PCD ETL Pipeline',
    schedule_interval=Variable.get('pcd_etl_schedule', default_var=None),
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'pcd'],
) as dag:

    check_pcd_sftp_folder = KubernetesJobOperator(
        task_id='check_pcd_sftp_folder',
        job_template_file=Variable.get('pcd_emtysftp_job'),
        wait_until_job_complete=True,
    )

    check_pcd_shared_folder = KubernetesJobOperator(
        task_id='check_pcd_shared_folder',
        job_template_file=Variable.get('pcd_emtydir_job'),
        wait_until_job_complete=True,
    )

    start_pcd_extract_1 = EmptyOperator(task_id='start_pcd_extract_1')

    http_api_tasks = []
    for endpoint in [
        'Financial_Expense',
        'UPCC_Financial_Reporting',
        'CHC_Financial_reporting',
        'PCN_Financial_Reporting',
        'NPPCC_Financial_Reporting',
        'Fiscal_Year_Reporting_Dates',
        'UPCC_Primary_Care_Patient_Services',
        'CHC_Primary_Care_Patient_Services',
        'Practitioner_Role_Mapping',
        'Status_Tracker',
        'HR_Records',
        'Provincial_Risk_Tracking',
        'Decision_Log',
        'HA_Hierarchy',
        'UPPC_Budget',
        'CHC_Budget',
        'PCN_Budget',
        'NPPCC_Budget',
    ]:
        http_api_tasks.append(
            SimpleHttpOperator(
                task_id=f'extract_{endpoint}',
                http_conn_id='http_default',
                endpoint=Variable.get(f'pcd_{endpoint.lower()}_url'),
                method='POST',
                data={"health_authority": "example"},
                response_check=lambda response: response.status_code == 200,
            )
        )

    start_pcd_extract_2 = EmptyOperator(task_id='start_pcd_extract_2')

    pcd_file_upload = KubernetesJobOperator(
        task_id='pcd_file_upload',
        job_template_file=Variable.get('pcd_job'),
    )

    etl_notification = PythonOperator(
        task_id='etl_notification',
        python_callable=send_etl_notification,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    check_pcd_sftp_folder >> check_pcd_shared_folder >> start_pcd_extract_1
    start_pcd_extract_1 >> http_api_tasks
    http_api_tasks >> start_pcd_extract_2
    start_pcd_extract_2 >> pcd_file_upload >> etl_notification