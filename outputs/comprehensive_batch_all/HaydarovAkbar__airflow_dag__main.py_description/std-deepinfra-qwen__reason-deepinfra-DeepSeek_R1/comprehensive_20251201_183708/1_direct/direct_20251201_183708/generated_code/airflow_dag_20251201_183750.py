from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.sql import SqlSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timedelta
import pytz

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1, tzinfo=pytz.timezone('Asia/Tashkent')),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['admin@example.com'],
    'max_active_runs': 20,
    'catchup': False
}

def get_load_id():
    """Retrieve a unique load identifier for session tracking."""
    # Example implementation
    return 'load_id_12345'

def workflow_registration(load_id):
    """Register the workflow session in metadata tables and initiate logging."""
    # Example implementation
    print(f"Workflow registered with load ID: {load_id}")

def send_flg_to_sap(row_count):
    """Send completion flag to SAP system with row count from segmentation results."""
    http_hook = HttpHook(http_conn_id='sap_http_conn', method='POST')
    response = http_hook.run(endpoint='/sap/endpoint', json={'row_count': row_count})
    print(f"SAP notification sent with status code: {response.status_code}")

def update_metadata(load_id):
    """Update metadata tables to mark the workflow session as successfully completed."""
    # Example implementation
    print(f"Metadata updated for load ID: {load_id}")

def email_on_failure(context):
    """Send failure notification email."""
    # Example implementation
    print(f"Failure notification sent for task: {context['task_instance'].task_id}")

with DAG(
    dag_id='l2_data_processing_pipeline',
    schedule_interval=None,
    default_args=default_args,
    description='Main workflow for loading datasets from DWH L2 to L2 with segmentation processing and SAP integration',
    tags=['data_processing', 'segmentation', 'sap_integration']
) as dag:

    wait_for_l2_full_load = SqlSensor(
        task_id='wait_for_l2_full_load',
        conn_id='dwh_postgres_conn',
        sql="SELECT * FROM metadata_table WHERE load_status = 'completed'",
        poke_interval=60,
        timeout=100,
        mode='poke'
    )

    get_load_id_task = PythonOperator(
        task_id='get_load_id',
        python_callable=get_load_id
    )

    workflow_registration_task = PythonOperator(
        task_id='workflow_registration',
        python_callable=workflow_registration,
        op_kwargs={'load_id': '{{ ti.xcom_pull(task_ids="get_load_id") }}'}
    )

    wait_for_success_end = ExternalTaskSensor(
        task_id='wait_for_success_end',
        external_dag_id='l2_data_processing_pipeline',
        external_task_id='end',
        execution_delta=timedelta(days=1),
        poke_interval=60,
        timeout=100,
        mode='poke'
    )

    run_sys_kill_all_session_pg = TriggerDagRunOperator(
        task_id='run_sys_kill_all_session_pg',
        trigger_dag_id='sys_kill_all_session_pg',
        execution_date='{{ ds }}'
    )

    with TaskGroup(group_id='segmentation_group') as segmentation_group:
        load_ds_client_segmentation = TriggerDagRunOperator(
            task_id='load_ds_client_segmentation',
            trigger_dag_id='l1_to_l2_p_load_data_ds_client_segmentation_full',
            execution_date='{{ ds }}'
        )

        send_flg_to_sap_task = PythonOperator(
            task_id='send_flg_to_sap',
            python_callable=send_flg_to_sap,
            op_kwargs={'row_count': 1000}  # Example row count
        )

        load_ds_client_segmentation >> send_flg_to_sap_task

    run_wf_data_preparation_for_reports = TriggerDagRunOperator(
        task_id='run_wf_data_preparation_for_reports',
        trigger_dag_id='wf_data_preparation_for_reports',
        execution_date='{{ ds }}'
    )

    end = PythonOperator(
        task_id='end',
        python_callable=update_metadata,
        op_kwargs={'load_id': '{{ ti.xcom_pull(task_ids="get_load_id") }}'}
    )

    email_on_failure_task = EmailOperator(
        task_id='email_on_failure',
        to='admin@example.com',
        subject='Pipeline Failure',
        html_content='The pipeline has failed. Please check the logs.',
        trigger_rule='one_failed'
    )

    wait_for_l2_full_load >> get_load_id_task >> workflow_registration_task >> wait_for_success_end >> run_sys_kill_all_session_pg
    run_sys_kill_all_session_pg >> [run_wf_data_preparation_for_reports, segmentation_group] >> end
    end >> email_on_failure_task