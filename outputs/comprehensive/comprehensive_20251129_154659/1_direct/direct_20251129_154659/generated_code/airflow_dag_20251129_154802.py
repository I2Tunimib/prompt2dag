from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import timedelta
from airflow.models import Variable
import pendulum

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
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
)

def send_etl_notification(**kwargs):
    dag_run = kwargs['dag_run']
    if dag_run.state == 'success':
        subject = "PCD ETL Pipeline - Success"
        html_content = "The PCD ETL pipeline has completed successfully."
    else:
        subject = "PCD ETL Pipeline - Failure"
        html_content = "The PCD ETL pipeline has failed."
    
    recipients = Variable.get('pcd_etl_notification_recipients', default_var='user@example.com')
    EmailOperator(
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
    cmds=['bash', '-c', 'your-check-sftp-script.sh'],
    name='check-pcd-sftp-folder',
    dag=dag,
)

check_pcd_shared_folder = KubernetesPodOperator(
    task_id='check_pcd_shared_folder',
    namespace='default',
    image='your-kubernetes-image',
    cmds=['bash', '-c', 'your-check-shared-folder-script.sh'],
    name='check-pcd-shared-folder',
    dag=dag,
)

start_pcd_extract_1 = DummyOperator(
    task_id='start_pcd_extract_1',
    dag=dag,
)

status_tracker = PythonOperator(
    task_id='status_tracker',
    python_callable=lambda: None,  # Placeholder for actual status tracking logic
    dag=dag,
)

financial_expense = PythonOperator(
    task_id='financial_expense',
    python_callable=lambda: None,  # Placeholder for actual financial expense extraction logic
    dag=dag,
)

upcc_financial_reporting = PythonOperator(
    task_id='upcc_financial_reporting',
    python_callable=lambda: None,  # Placeholder for actual UPCC financial reporting extraction logic
    dag=dag,
)

chc_financial_reporting = PythonOperator(
    task_id='chc_financial_reporting',
    python_callable=lambda: None,  # Placeholder for actual CHC financial reporting extraction logic
    dag=dag,
)

pcn_financial_reporting = PythonOperator(
    task_id='pcn_financial_reporting',
    python_callable=lambda: None,  # Placeholder for actual PCN financial reporting extraction logic
    dag=dag,
)

nppcc_financial_reporting = PythonOperator(
    task_id='nppcc_financial_reporting',
    python_callable=lambda: None,  # Placeholder for actual NPPCC financial reporting extraction logic
    dag=dag,
)

fiscal_year_reporting_dates = PythonOperator(
    task_id='fiscal_year_reporting_dates',
    python_callable=lambda: None,  # Placeholder for actual fiscal year reporting dates extraction logic
    dag=dag,
)

upcc_primary_care_patient_services = PythonOperator(
    task_id='upcc_primary_care_patient_services',
    python_callable=lambda: None,  # Placeholder for actual UPCC primary care patient services extraction logic
    dag=dag,
)

chc_primary_care_patient_services = PythonOperator(
    task_id='chc_primary_care_patient_services',
    python_callable=lambda: None,  # Placeholder for actual CHC primary care patient services extraction logic
    dag=dag,
)

practitioner_role_mapping = PythonOperator(
    task_id='practitioner_role_mapping',
    python_callable=lambda: None,  # Placeholder for actual practitioner role mapping extraction logic
    dag=dag,
)

hr_records = PythonOperator(
    task_id='hr_records',
    python_callable=lambda: None,  # Placeholder for actual HR records extraction logic
    dag=dag,
)

provincial_risk_tracking = PythonOperator(
    task_id='provincial_risk_tracking',
    python_callable=lambda: None,  # Placeholder for actual provincial risk tracking extraction logic
    dag=dag,
)

decision_log = PythonOperator(
    task_id='decision_log',
    python_callable=lambda: None,  # Placeholder for actual decision log extraction logic
    dag=dag,
)

ha_hierarchy = PythonOperator(
    task_id='ha_hierarchy',
    python_callable=lambda: None,  # Placeholder for actual HA hierarchy extraction logic
    dag=dag,
)

uppc_budget = PythonOperator(
    task_id='uppc_budget',
    python_callable=lambda: None,  # Placeholder for actual UPPC budget extraction logic
    dag=dag,
)

chc_budget = PythonOperator(
    task_id='chc_budget',
    python_callable=lambda: None,  # Placeholder for actual CHC budget extraction logic
    dag=dag,
)

pcn_budget = PythonOperator(
    task_id='pcn_budget',
    python_callable=lambda: None,  # Placeholder for actual PCN budget extraction logic
    dag=dag,
)

nppcc_budget = PythonOperator(
    task_id='nppcc_budget',
    python_callable=lambda: None,  # Placeholder for actual NPPCC budget extraction logic
    dag=dag,
)

start_pcd_extract_2 = DummyOperator(
    task_id='start_pcd_extract_2',
    dag=dag,
)

pcd_file_upload = KubernetesPodOperator(
    task_id='pcd_file_upload',
    namespace='default',
    image='your-kubernetes-image',
    cmds=['bash', '-c', 'your-file-upload-script.sh'],
    name='pcd-file-upload',
    dag=dag,
)

etl_notification = PythonOperator(
    task_id='etl_notification',
    python_callable=send_etl_notification,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# Define task dependencies
check_pcd_shared_folder >> start_pcd_extract_1
start_pcd_extract_1 >> status_tracker
status_tracker >> start_pcd_extract_2

start_pcd_extract_1 >> [
    financial_expense,
    upcc_financial_reporting,
    chc_financial_reporting,
    pcn_financial_reporting,
    nppcc_financial_reporting,
    fiscal_year_reporting_dates,
    upcc_primary_care_patient_services,
    chc_primary_care_patient_services,
    practitioner_role_mapping,
    hr_records,
    provincial_risk_tracking,
    decision_log,
    ha_hierarchy,
    uppc_budget,
    chc_budget,
    pcn_budget,
    nppcc_budget,
]

start_pcd_extract_2 >> pcd_file_upload

[
    financial_expense,
    upcc_financial_reporting,
    chc_financial_reporting,
    pcn_financial_reporting,
    nppcc_financial_reporting,
    fiscal_year_reporting_dates,
    upcc_primary_care_patient_services,
    chc_primary_care_patient_services,
    practitioner_role_mapping,
    hr_records,
    provincial_risk_tracking,
    decision_log,
    ha_hierarchy,
    uppc_budget,
    chc_budget,
    pcn_budget,
    nppcc_budget,
] >> pcd_file_upload

pcd_file_upload >> etl_notification