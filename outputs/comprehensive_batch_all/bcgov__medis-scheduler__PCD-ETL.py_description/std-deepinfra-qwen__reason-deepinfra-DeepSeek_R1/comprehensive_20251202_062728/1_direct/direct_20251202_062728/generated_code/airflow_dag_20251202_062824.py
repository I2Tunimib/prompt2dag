from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta
from airflow.models import Variable
import datetime

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
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

def send_notification(**context):
    dag_run = context['dag_run']
    if dag_run.state == 'success':
        subject = "PCD ETL Pipeline - Success"
        html_content = "The PCD ETL pipeline has completed successfully."
    else:
        subject = "PCD ETL Pipeline - Failure"
        html_content = "The PCD ETL pipeline has failed."
    recipients = Variable.get('pcd_etl_notification_recipients', default_var='example@example.com')
    EmailOperator(
        task_id='send_notification',
        to=recipients,
        subject=subject,
        html_content=html_content,
        dag=dag,
    ).execute(context)

check_pcd_sftp_folder = KubernetesPodOperator(
    task_id='check_pcd_sftp_folder',
    namespace='default',
    image='your-k8s-image',
    cmds=['bash', '-c'],
    arguments=['your-sftp-check-command'],
    name='check-pcd-sftp-folder',
    dag=dag,
)

check_pcd_shared_folder = KubernetesPodOperator(
    task_id='check_pcd_shared_folder',
    namespace='default',
    image='your-k8s-image',
    cmds=['bash', '-c'],
    arguments=['your-shared-folder-check-command'],
    name='check-pcd-shared-folder',
    dag=dag,
)

start_pcd_extract_1 = DummyOperator(
    task_id='start_pcd_extract_1',
    dag=dag,
)

status_tracker = SimpleHttpOperator(
    task_id='status_tracker',
    http_conn_id='http_conn_id',
    endpoint=Variable.get('status_tracker_endpoint', default_var=''),
    method='GET',
    dag=dag,
)

financial_expense = SimpleHttpOperator(
    task_id='financial_expense',
    http_conn_id='http_conn_id',
    endpoint=Variable.get('financial_expense_endpoint', default_var=''),
    method='GET',
    dag=dag,
)

upcc_financial_reporting = SimpleHttpOperator(
    task_id='upcc_financial_reporting',
    http_conn_id='http_conn_id',
    endpoint=Variable.get('upcc_financial_reporting_endpoint', default_var=''),
    method='GET',
    dag=dag,
)

chc_financial_reporting = SimpleHttpOperator(
    task_id='chc_financial_reporting',
    http_conn_id='http_conn_id',
    endpoint=Variable.get('chc_financial_reporting_endpoint', default_var=''),
    method='GET',
    dag=dag,
)

pcn_financial_reporting = SimpleHttpOperator(
    task_id='pcn_financial_reporting',
    http_conn_id='http_conn_id',
    endpoint=Variable.get('pcn_financial_reporting_endpoint', default_var=''),
    method='GET',
    dag=dag,
)

nppcc_financial_reporting = SimpleHttpOperator(
    task_id='nppcc_financial_reporting',
    http_conn_id='http_conn_id',
    endpoint=Variable.get('nppcc_financial_reporting_endpoint', default_var=''),
    method='GET',
    dag=dag,
)

fiscal_year_reporting_dates = SimpleHttpOperator(
    task_id='fiscal_year_reporting_dates',
    http_conn_id='http_conn_id',
    endpoint=Variable.get('fiscal_year_reporting_dates_endpoint', default_var=''),
    method='GET',
    dag=dag,
)

upcc_primary_care_patient_services = SimpleHttpOperator(
    task_id='upcc_primary_care_patient_services',
    http_conn_id='http_conn_id',
    endpoint=Variable.get('upcc_primary_care_patient_services_endpoint', default_var=''),
    method='GET',
    dag=dag,
)

chc_primary_care_patient_services = SimpleHttpOperator(
    task_id='chc_primary_care_patient_services',
    http_conn_id='http_conn_id',
    endpoint=Variable.get('chc_primary_care_patient_services_endpoint', default_var=''),
    method='GET',
    dag=dag,
)

practitioner_role_mapping = SimpleHttpOperator(
    task_id='practitioner_role_mapping',
    http_conn_id='http_conn_id',
    endpoint=Variable.get('practitioner_role_mapping_endpoint', default_var=''),
    method='GET',
    dag=dag,
)

hr_records = SimpleHttpOperator(
    task_id='hr_records',
    http_conn_id='http_conn_id',
    endpoint=Variable.get('hr_records_endpoint', default_var=''),
    method='GET',
    dag=dag,
)

provincial_risk_tracking = SimpleHttpOperator(
    task_id='provincial_risk_tracking',
    http_conn_id='http_conn_id',
    endpoint=Variable.get('provincial_risk_tracking_endpoint', default_var=''),
    method='GET',
    dag=dag,
)

decision_log = SimpleHttpOperator(
    task_id='decision_log',
    http_conn_id='http_conn_id',
    endpoint=Variable.get('decision_log_endpoint', default_var=''),
    method='GET',
    dag=dag,
)

ha_hierarchy = SimpleHttpOperator(
    task_id='ha_hierarchy',
    http_conn_id='http_conn_id',
    endpoint=Variable.get('ha_hierarchy_endpoint', default_var=''),
    method='GET',
    dag=dag,
)

uppc_budget = SimpleHttpOperator(
    task_id='uppc_budget',
    http_conn_id='http_conn_id',
    endpoint=Variable.get('uppc_budget_endpoint', default_var=''),
    method='GET',
    dag=dag,
)

chc_budget = SimpleHttpOperator(
    task_id='chc_budget',
    http_conn_id='http_conn_id',
    endpoint=Variable.get('chc_budget_endpoint', default_var=''),
    method='GET',
    dag=dag,
)

pcn_budget = SimpleHttpOperator(
    task_id='pcn_budget',
    http_conn_id='http_conn_id',
    endpoint=Variable.get('pcn_budget_endpoint', default_var=''),
    method='GET',
    dag=dag,
)

nppcc_budget = SimpleHttpOperator(
    task_id='nppcc_budget',
    http_conn_id='http_conn_id',
    endpoint=Variable.get('nppcc_budget_endpoint', default_var=''),
    method='GET',
    dag=dag,
)

start_pcd_extract_2 = DummyOperator(
    task_id='start_pcd_extract_2',
    dag=dag,
)

pcd_file_upload = KubernetesPodOperator(
    task_id='pcd_file_upload',
    namespace='default',
    image='your-k8s-image',
    cmds=['bash', '-c'],
    arguments=['your-file-upload-command'],
    name='pcd-file-upload',
    dag=dag,
)

etl_notification = PythonOperator(
    task_id='etl_notification',
    python_callable=send_notification,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# Define task dependencies
check_pcd_shared_folder >> start_pcd_extract_1
start_pcd_extract_1 >> [status_tracker, financial_expense, upcc_financial_reporting, chc_financial_reporting, pcn_financial_reporting, nppcc_financial_reporting, fiscal_year_reporting_dates, upcc_primary_care_patient_services, chc_primary_care_patient_services, practitioner_role_mapping, hr_records, provincial_risk_tracking, decision_log, ha_hierarchy, uppc_budget, chc_budget, pcn_budget, nppcc_budget]
status_tracker >> start_pcd_extract_2
start_pcd_extract_2 >> pcd_file_upload
pcd_file_upload >> etl_notification