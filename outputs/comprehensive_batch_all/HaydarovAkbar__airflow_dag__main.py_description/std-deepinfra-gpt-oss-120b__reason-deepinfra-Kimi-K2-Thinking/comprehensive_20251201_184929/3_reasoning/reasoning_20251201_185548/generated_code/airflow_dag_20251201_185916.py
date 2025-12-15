from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.sql import SqlSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import pendulum

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email': ['data-team@company.com', 'admin@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dwh_l1_to_l2_main_workflow',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1, tzinfo=pendulum.timezone('Asia/Tashkent')),
    catchup=False,
    max_active_runs=20,
    default_args=default_args,
    tags=['dwh', 'l1-l2', 'segmentation', 'sap', 'main-workflow'],
    doc_md="Main workflow for loading datasets from DWH L1 to L2 with segmentation processing and SAP integration.",
) as dag:
    
    wait_for_l2_full_load = SqlSensor(
        task_id='wait_for_l2_full_load',
        conn_id='postgres_dwh',
        sql="SELECT load_completed FROM metadata.l1_l2_load_status WHERE load_date = CURRENT_DATE AND status = 'SUCCESS'",
        poke_interval=60,
        timeout=3600,
        mode='poke',
    )
    
    def get_load_id(**context):
        """Retrieve or generate a unique load identifier for session tracking."""
        from datetime import datetime
        load_id = f"load_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{context['run_id']}"
        context['task_instance'].xcom_push(key='load_id', value=load_id)
        return load_id
    
    get_load_id_task = PythonOperator(
        task_id='get_load_id',
        python_callable=get_load_id,
    )
    
    def workflow_registration(**context):
        """Register the workflow session in metadata tables and initiate logging."""
        load_id = context['task_instance'].xcom_pull(task_ids='get_load_id', key='load_id')
        print(f"Registering workflow session for load_id: {load