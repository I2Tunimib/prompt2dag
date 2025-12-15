from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'pcd-data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

schedule_interval = Variable.get("pcd_etl_schedule", default_var=None)

with DAG(
    dag_id='pcd_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Primary Care Data extraction and processing',
    schedule_interval=schedule_interval,
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=['pcd', 'etl', 'kubernetes'],
    max_active_runs=1,
) as dag:

    check_pcd_sftp_folder = KubernetesPodOperator(
        task_id='check_pcd_sftp_folder',
        namespace=Variable.get("pcd_kubernetes_namespace", default_var="default"),
        image='busybox:latest',
        cmds=['sh', '-c'],
        arguments=[
            'echo "Checking SFTP folder for PCD files"; '
            'wget -O - {{ var.value.pcd_sftp_check_job_template }} | sh'
        ],
        name='check-pcd-sftp-folder',
        get_logs=True,
        is_delete_operator_pod=True,
    )

    check_pcd_shared_folder = KubernetesPodOperator(
        task_id='check_pcd_shared_folder',
        namespace=Variable.get("pcd_kubernetes_namespace", default_var="default"),
        image='busybox:latest',
        cmds=['sh', '-c'],
        arguments=[
            'echo "Checking shared folder for PCD files"; '
            'wget -O - {{ var.value.pcd_shared_folder_check_job_template }} | sh'
        ],
        name='check-pcd-shared-folder',
        get_logs=True,
        is_delete_operator_pod=True,
    )

    start_pcd_extract_1 = EmptyOperator(task_id='start_pcd_extract_1')

    def extract_status_tracker(**context):
        print("Extracting status tracker data from HTTP API")

    def extract_financial_expense(**context):
        print("Extracting financial expense data from HTTP API")

    def extract_upcc_financial_reporting(**context):
        print("Extracting UPCC financial reporting data from HTTP API")

    def extract_chc_financial_reporting(**context):
        print("Extracting CHC financial reporting data from HTTP API")

    def extract_pcn_financial_reporting(**context):
        print("Extracting PCN financial reporting data from HTTP API")

    def extract_nppcc_financial_reporting(**context):
        print("Extracting NPPCC financial reporting data from HTTP API")

    def extract_fiscal_year_reporting_dates(**context):
        print("Extracting fiscal year reporting dates from HTTP API")

    def extract_upcc_primary_care_patient_services(**context):
        print("Extracting UPCC primary care patient services data from HTTP API")

    def extract_chc_primary_care_patient_services(**context):
        print("Extracting CHC primary care patient services data from HTTP API")

    def extract_practitioner_role_mapping(**context):
        print("Extracting practitioner role mapping data from HTTP API")

    def extract_hr_records(**context):
        print("Extracting HR records data from HTTP API")

    def extract_provincial_risk_tracking(**context):
        print("Extracting provincial risk tracking data from HTTP API")

    def extract_decision_log(**context):
        print("Extracting decision log data from HTTP API")

    def extract_ha_hierarchy(**context):
        print("Extracting HA hierarchy data from HTTP API")

    def extract_uppc_budget(**context):
        print("Extracting UPPC budget data from HTTP API")

    def extract_chc_budget(**context):
        print("Extracting CHC budget data from HTTP API")

    def extract_pcn_budget(**context):
        print("Extracting PCN budget data from HTTP API")

    def extract_nppcc_budget(**context):
        print("Extracting NPPCC budget data from HTTP API")

    status_tracker = PythonOperator(
        task_id='status_tracker',
        python_callable=extract_status_tracker,
    )

    financial_expense = PythonOperator(
        task_id='financial_expense',
        python_callable=extract_financial_expense,
    )

    upcc_financial_reportingr = PythonOperator(
        task_id='upcc_financial_reportingr',
        python_callable=extract_upcc_financial_reporting,
    )

    chc_financial_reporting = PythonOperator(
        task_id='chc_financial_reporting',
        python_callable=extract_chc_financial_reporting,
    )

    pcn_financial_reporting = PythonOperator(
        task_id='pcn_financial_reporting',
        python_callable=extract_pcn_financial_reporting,
    )

    nppcc_financial_reporting = PythonOperator(
        task_id='nppcc_financial_reporting',
        python_callable=extract_nppcc_financial_reporting,
    )

    fiscal_year_reporting_dates = PythonOperator(
        task_id='fiscal_year_reporting_dates',
        python_callable=extract_fiscal_year_reporting_dates,
    )

    upcc_primary_care_patient_services = PythonOperator(
        task_id='upcc_primary_care_patient_services',
        python_callable=extract_upcc_primary_care_patient_services,
    )

    chc_primary_care_patient_services = PythonOperator(
        task_id='chc_primary_care_patient_services',
        python_callable=extract_chc_primary_care_patient_services,
    )

    practitioner_role_mapping = PythonOperator(
        task_id='practitioner_role_mapping',
        python_callable=extract_practitioner_role_mapping,
    )

    hr_records = PythonOperator(
        task_id='hr_records',
        python_callable=extract_hr_records,
    )

    provincial_risk_tracking = PythonOperator(
        task_id='provincial_risk_tracking',
        python_callable=extract_provincial_risk_tracking,
    )

    decision_log = PythonOperator(
        task_id='decision_log',
        python_callable=extract_decision_log,
    )

    ha_hierarchy = PythonOperator(
        task_id='ha_hierarchy',
        python_callable=extract_ha_hierarchy,
    )

    uppc_budget = PythonOperator(
        task_id='uppc_budget',
        python_callable=extract_uppc_budget,
    )

    chc_budget = PythonOperator(
        task_id='chc_budget',
        python_callable=extract_chc_budget,
    )

    pcn_budget = PythonOperator(
        task_id='pcn_budget',
        python_callable=extract_pcn_budget,
    )

    nppcc_budget = PythonOperator(
        task_id='nppcc_budget',
        python_callable=extract_nppcc_budget,
    )

    start_pcd_extract_2 = EmptyOperator(task_id='start_pcd_extract_2')

    pcd_file_upload = KubernetesPodOperator(
        task_id='pcd_file_upload',
        namespace=Variable.get("pcd_kubernetes_namespace", default_var="default"),
        image='busybox:latest',
        cmds=['sh', '-c'],
        arguments=[
            'echo "Performing PCD file upload"; '
            'wget -O - {{ var.value.pcd_file_upload_job_template }} | sh'
        ],
        name='pcd-file-upload',
        get_logs=True,
        is_delete_operator_pod=True,
    )

    def send_etl_notification(**context):
        dag_run = context['dag_run']
        task_instances = dag_run.get_task_instances()
        failed_tasks = [ti for ti in task_instances if ti.state == 'failed']
        
        if failed_tasks:
            print(f"DAG run {dag_run.run_id} completed with failures: {[ti.task_id for ti in failed_tasks]}")
        else:
            print(f"DAG run {dag_run.run_id} completed successfully")
        
        # Integrate with Airflow email backend using distribution lists from Variables
        # email_to = Variable.get("pcd_notification_email_list", default_var="").split(',')

    etl_notification = PythonOperator(
        task_id='etl_notification',
        python_callable=send_etl_notification,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Task dependencies
    check_pcd_sftp_folder >> check_pcd_shared_folder >> start_pcd_extract_1
    
    start_pcd_extract_1 >> [
        status_tracker,
        financial_expense,
        upcc_financial_reportingr,
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
    
    status_tracker >> start_pcd_extract_2
    
    [
        status_tracker,
        financial_expense,
        upcc_financial_reportingr,
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
    ] >> pcd_file_upload >> etl_notification