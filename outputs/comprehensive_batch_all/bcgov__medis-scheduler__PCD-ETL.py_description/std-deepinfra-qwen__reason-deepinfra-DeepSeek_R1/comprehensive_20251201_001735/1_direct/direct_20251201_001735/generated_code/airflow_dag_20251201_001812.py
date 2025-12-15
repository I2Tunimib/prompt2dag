from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta
from airflow.models import Variable
import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=60),
}

dag = DAG(
    'pcd_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Primary Care Data',
    schedule_interval=Variable.get('pcd_etl_schedule', default_var=None),
    catchup=False,
    start_date=datetime.datetime(2023, 1, 1),
)

def send_etl_notification(**kwargs):
    dag_run = kwargs['dag_run']
    if dag_run.state == 'success':
        subject = "ETL Pipeline Success"
        html_content = "The ETL pipeline has completed successfully."
    else:
        subject = "ETL Pipeline Failure"
        html_content = "The ETL pipeline has failed."
    recipients = Variable.get('etl_notification_recipients', default_var='user@example.com')
    return EmailOperator(
        task_id='send_etl_notification',
        to=recipients,
        subject=subject,
        html_content=html_content,
        dag=dag,
    ).execute(kwargs)

check_pcd_sftp_folder = KubernetesPodOperator(
    task_id='check_pcd_sftp_folder',
    namespace='default',
    image='your-kubernetes-image',
    name='check-pcd-sftp-folder',
    cmds=['bash', '-c', 'your-check-sftp-script.sh'],
    dag=dag,
)

check_pcd_shared_folder = KubernetesPodOperator(
    task_id='check_pcd_shared_folder',
    namespace='default',
    image='your-kubernetes-image',
    name='check-pcd-shared-folder',
    cmds=['bash', '-c', 'your-check-shared-folder-script.sh'],
    dag=dag,
)

start_pcd_extract_1 = DummyOperator(task_id='start_pcd_extract_1', dag=dag)

status_tracker = DummyOperator(task_id='status_tracker', dag=dag)

financial_expense = DummyOperator(task_id='financial_expense', dag=dag)
upcc_financial_reporting = DummyOperator(task_id='upcc_financial_reporting', dag=dag)
chc_financial_reporting = DummyOperator(task_id='chc_financial_reporting', dag=dag)
pcn_financial_reporting = DummyOperator(task_id='pcn_financial_reporting', dag=dag)
nppcc_financial_reporting = DummyOperator(task_id='nppcc_financial_reporting', dag=dag)
fiscal_year_reporting_dates = DummyOperator(task_id='fiscal_year_reporting_dates', dag=dag)
upcc_primary_care_patient_services = DummyOperator(task_id='upcc_primary_care_patient_services', dag=dag)
chc_primary_care_patient_services = DummyOperator(task_id='chc_primary_care_patient_services', dag=dag)
practitioner_role_mapping = DummyOperator(task_id='practitioner_role_mapping', dag=dag)
hr_records = DummyOperator(task_id='hr_records', dag=dag)
provincial_risk_tracking = DummyOperator(task_id='provincial_risk_tracking', dag=dag)
decision_log = DummyOperator(task_id='decision_log', dag=dag)
ha_hierarchy = DummyOperator(task_id='ha_hierarchy', dag=dag)
uppc_budget = DummyOperator(task_id='uppc_budget', dag=dag)
chc_budget = DummyOperator(task_id='chc_budget', dag=dag)
pcn_budget = DummyOperator(task_id='pcn_budget', dag=dag)
nppcc_budget = DummyOperator(task_id='nppcc_budget', dag=dag)

start_pcd_extract_2 = DummyOperator(task_id='start_pcd_extract_2', dag=dag)

pcd_file_upload = KubernetesPodOperator(
    task_id='pcd_file_upload',
    namespace='default',
    image='your-kubernetes-image',
    name='pcd-file-upload',
    cmds=['bash', '-c', 'your-file-upload-script.sh'],
    dag=dag,
)

etl_notification = PythonOperator(
    task_id='etl_notification',
    python_callable=send_etl_notification,
    provide_context=True,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# Define task dependencies
check_pcd_shared_folder >> start_pcd_extract_1
start_pcd_extract_1 >> status_tracker
status_tracker >> start_pcd_extract_2

start_pcd_extract_1 >> financial_expense
start_pcd_extract_1 >> upcc_financial_reporting
start_pcd_extract_1 >> chc_financial_reporting
start_pcd_extract_1 >> pcn_financial_reporting
start_pcd_extract_1 >> nppcc_financial_reporting
start_pcd_extract_1 >> fiscal_year_reporting_dates
start_pcd_extract_1 >> upcc_primary_care_patient_services
start_pcd_extract_1 >> chc_primary_care_patient_services
start_pcd_extract_1 >> practitioner_role_mapping
start_pcd_extract_1 >> hr_records
start_pcd_extract_1 >> provincial_risk_tracking
start_pcd_extract_1 >> decision_log
start_pcd_extract_1 >> ha_hierarchy
start_pcd_extract_1 >> uppc_budget
start_pcd_extract_1 >> chc_budget
start_pcd_extract_1 >> pcn_budget
start_pcd_extract_1 >> nppcc_budget

start_pcd_extract_2 >> pcd_file_upload

pcd_file_upload >> etl_notification