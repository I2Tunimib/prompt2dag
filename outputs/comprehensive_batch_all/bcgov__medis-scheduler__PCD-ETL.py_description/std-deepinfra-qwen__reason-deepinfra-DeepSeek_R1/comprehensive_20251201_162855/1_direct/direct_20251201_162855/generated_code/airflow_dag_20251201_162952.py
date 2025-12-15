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
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
)

def send_etl_notification(**kwargs):
    dag_run = kwargs['dag_run']
    if dag_run.state == 'success':
        subject = "ETL Pipeline Success"
        html_content = "The ETL pipeline has completed successfully."
    else:
        subject = "ETL Pipeline Failure"
        html_content = "The ETL pipeline has failed."
    
    recipients = Variable.get('etl_notification_recipients', default_var='example@example.com')
    EmailOperator(
        task_id='send_etl_notification',
        to=recipients,
        subject=subject,
        html_content=html_content,
        dag=dag
    ).execute(kwargs)

check_pcd_sftp_folder = KubernetesPodOperator(
    task_id='check_pcd_sftp_folder',
    namespace='default',
    image='your-kubernetes-image',
    cmds=['bash', '-c'],
    arguments=['your-sftp-check-command'],
    name='check-pcd-sftp-folder',
    dag=dag,
)

check_pcd_shared_folder = KubernetesPodOperator(
    task_id='check_pcd_shared_folder',
    namespace='default',
    image='your-kubernetes-image',
    cmds=['bash', '-c'],
    arguments=['your-shared-folder-check-command'],
    name='check-pcd-shared-folder',
    dag=dag,
)

start_pcd_extract_1 = DummyOperator(task_id='start_pcd_extract_1', dag=dag)

status_tracker = KubernetesPodOperator(
    task_id='status_tracker',
    namespace='default',
    image='your-kubernetes-image',
    cmds=['bash', '-c'],
    arguments=['your-status-tracker-command'],
    name='status-tracker',
    dag=dag,
)

financial_expense = KubernetesPodOperator(
    task_id='financial_expense',
    namespace='default',
    image='your-kubernetes-image',
    cmds=['bash', '-c'],
    arguments=['your-financial-expense-command'],
    name='financial-expense',
    dag=dag,
)

upcc_financial_reporting = KubernetesPodOperator(
    task_id='upcc_financial_reporting',
    namespace='default',
    image='your-kubernetes-image',
    cmds=['bash', '-c'],
    arguments=['your-upcc-financial-reporting-command'],
    name='upcc-financial-reporting',
    dag=dag,
)

chc_financial_reporting = KubernetesPodOperator(
    task_id='chc_financial_reporting',
    namespace='default',
    image='your-kubernetes-image',
    cmds=['bash', '-c'],
    arguments=['your-chc-financial-reporting-command'],
    name='chc-financial-reporting',
    dag=dag,
)

pcn_financial_reporting = KubernetesPodOperator(
    task_id='pcn_financial_reporting',
    namespace='default',
    image='your-kubernetes-image',
    cmds=['bash', '-c'],
    arguments=['your-pcn-financial-reporting-command'],
    name='pcn-financial-reporting',
    dag=dag,
)

nppcc_financial_reporting = KubernetesPodOperator(
    task_id='nppcc_financial_reporting',
    namespace='default',
    image='your-kubernetes-image',
    cmds=['bash', '-c'],
    arguments=['your-nppcc-financial-reporting-command'],
    name='nppcc-financial-reporting',
    dag=dag,
)

fiscal_year_reporting_dates = KubernetesPodOperator(
    task_id='fiscal_year_reporting_dates',
    namespace='default',
    image='your-kubernetes-image',
    cmds=['bash', '-c'],
    arguments=['your-fiscal-year-reporting-dates-command'],
    name='fiscal-year-reporting-dates',
    dag=dag,
)

upcc_primary_care_patient_services = KubernetesPodOperator(
    task_id='upcc_primary_care_patient_services',
    namespace='default',
    image='your-kubernetes-image',
    cmds=['bash', '-c'],
    arguments=['your-upcc-primary-care-patient-services-command'],
    name='upcc-primary-care-patient-services',
    dag=dag,
)

chc_primary_care_patient_services = KubernetesPodOperator(
    task_id='chc_primary_care_patient_services',
    namespace='default',
    image='your-kubernetes-image',
    cmds=['bash', '-c'],
    arguments=['your-chc-primary-care-patient-services-command'],
    name='chc-primary-care-patient-services',
    dag=dag,
)

practitioner_role_mapping = KubernetesPodOperator(
    task_id='practitioner_role_mapping',
    namespace='default',
    image='your-kubernetes-image',
    cmds=['bash', '-c'],
    arguments=['your-practitioner-role-mapping-command'],
    name='practitioner-role-mapping',
    dag=dag,
)

hr_records = KubernetesPodOperator(
    task_id='hr_records',
    namespace='default',
    image='your-kubernetes-image',
    cmds=['bash', '-c'],
    arguments=['your-hr-records-command'],
    name='hr-records',
    dag=dag,
)

provincial_risk_tracking = KubernetesPodOperator(
    task_id='provincial_risk_tracking',
    namespace='default',
    image='your-kubernetes-image',
    cmds=['bash', '-c'],
    arguments=['your-provincial-risk-tracking-command'],
    name='provincial-risk-tracking',
    dag=dag,
)

decision_log = KubernetesPodOperator(
    task_id='decision_log',
    namespace='default',
    image='your-kubernetes-image',
    cmds=['bash', '-c'],
    arguments=['your-decision-log-command'],
    name='decision-log',
    dag=dag,
)

ha_hierarchy = KubernetesPodOperator(
    task_id='ha_hierarchy',
    namespace='default',
    image='your-kubernetes-image',
    cmds=['bash', '-c'],
    arguments=['your-ha-hierarchy-command'],
    name='ha-hierarchy',
    dag=dag,
)

uppc_budget = KubernetesPodOperator(
    task_id='uppc_budget',
    namespace='default',
    image='your-kubernetes-image',
    cmds=['bash', '-c'],
    arguments=['your-uppc-budget-command'],
    name='uppc-budget',
    dag=dag,
)

chc_budget = KubernetesPodOperator(
    task_id='chc_budget',
    namespace='default',
    image='your-kubernetes-image',
    cmds=['bash', '-c'],
    arguments=['your-chc-budget-command'],
    name='chc-budget',
    dag=dag,
)

pcn_budget = KubernetesPodOperator(
    task_id='pcn_budget',
    namespace='default',
    image='your-kubernetes-image',
    cmds=['bash', '-c'],
    arguments=['your-pcn-budget-command'],
    name='pcn-budget',
    dag=dag,
)

nppcc_budget = KubernetesPodOperator(
    task_id='nppcc_budget',
    namespace='default',
    image='your-kubernetes-image',
    cmds=['bash', '-c'],
    arguments=['your-nppcc-budget-command'],
    name='nppcc-budget',
    dag=dag,
)

start_pcd_extract_2 = DummyOperator(task_id='start_pcd_extract_2', dag=dag)

pcd_file_upload = KubernetesPodOperator(
    task_id='pcd_file_upload',
    namespace='default',
    image='your-kubernetes-image',
    cmds=['bash', '-c'],
    arguments=['your-file-upload-command'],
    name='pcd-file-upload',
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
check_pcd_sftp_folder >> check_pcd_shared_folder >> start_pcd_extract_1
start_pcd_extract_1 >> status_tracker >> start_pcd_extract_2
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
    nppcc_budget
] >> start_pcd_extract_2
start_pcd_extract_2 >> pcd_file_upload >> etl_notification