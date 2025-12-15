import uuid
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.sql import SqlSensor
from airflow.utils.email import send_email
from airflow.utils.task_group import TaskGroup
from airflow.utils.timezone import make_aware


class md_dwh:
    """Placeholder for custom metadata module. Replace with actual implementation."""
    
    @staticmethod
    def register_session(context: Dict[str, Any]) -> None:
        """Register workflow session in metadata tables."""
        print(f"Registering workflow session: {context['run_id']}")
        
    @staticmethod
    def update_session_status(context: Dict[str, Any], status: str) -> None:
        """Update workflow session status in metadata tables."""
        print(f"Updating session status to {status}: {context['run_id']}")


def retrieve_load_id(**context) -> str:
    """Generate a unique load identifier for session tracking."""
    load_id = f"load_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    context['task_instance'].xcom_push(key='load_id', value=load_id)
    return load_id


def register_workflow_session(**context) -> None:
    """Register the workflow session in metadata tables and initiate logging."""
    md_dwh.register_session(context)


def send_flag_to_sap(**context) -> None:
    """Send completion flag to SAP system with row count from segmentation results."""
    load_id = context['task_instance'].xcom_pull(task_ids='get_load_id', key='load_id')
    
    # In production, query actual row count from segmentation results
    # row_count = get_segmentation_row_count(load_id)
    # send_sap_notification(load_id, row_count)
    
    print(f"Sending completion flag to SAP for load_id: {load_id}")
    print("SAP notification sent successfully")


def update_workflow_session_status(**context) -> None:
    """Update metadata tables to mark the workflow session as successfully completed."""
    md_dwh.update_session_status(context, 'COMPLETED')


def send_failure_notification(context: Dict[str, Any]) -> None:
    """Send email notification on DAG failure."""
    subject = f"DAG {context['dag'].dag_id} failed"
    body = f"""
    <h3>DAG Failure Notification</h3>
    <p><strong>DAG:</strong> {context['dag'].dag_id}</p>
    <p><strong>Run ID:</strong> {context['run_id']}</p>
    <p><strong>Execution Date:</strong> {context['execution_date']}</p>
    <p><strong>Failed Task:</strong> {context['task_instance'].task_id}</p>
    <p><strong>Error:</strong> {context.get('exception', 'N/A')}</p>
    """
    send_email(
        to=context['dag'].default_args['email'],
        subject=subject,
        html_content=body
    )


default_args = {
    'owner': 'data-engineering',
    'email': ['data-team@company.com'],
    'email_on_failure': True,
    'retries': 0,
}

with DAG(
    dag_id='dwh_l2_to_l2_segmentation_sap_pipeline',
    default_args=default_args,
    description='Main workflow for loading datasets from DWH L2 to L2 with segmentation processing and SAP integration',
    schedule_interval=None,
    catchup=False,
    max_active_runs=20,
    start_date=make_aware(datetime(2024, 1, 1)),
    timezone='Asia/Tashkent',
    on_failure_callback=send_failure_notification,
    tags=['dwh', 'l2', 'segmentation', 'sap'],
) as dag:
    
    wait_for_l2_full_load = SqlSensor(
        task_id='wait_for_l2_full_load',
        conn_id='postgres_dwh',
        sql="""
            SELECT 1 
            FROM metadata.load_flags 
            WHERE load_type = 'L1_TO_L2_FULL' 
            AND status = 'COMPLETED' 
            AND execution_date = '{{ ds }}'::date
        """,
        poke_interval=60,
        timeout=3600,
        mode='poke',
    )
    
    get_load_id = PythonOperator(
        task_id='get_load_id',
        python_callable=retrieve_load_id,
    )
    
    workflow_registration = PythonOperator(
        task_id='workflow_registration',
        python_callable=register_workflow_session,
    )
    
    wait_for_success_end = ExternalTaskSensor(
        task_id='wait_for_success_end',
        external_dag_id='dwh_l2_to_l2_segmentation_sap_pipeline',
        external_task_id='end',
        execution_delta=timedelta(hours=24),
        poke_interval=100,
        timeout=7200,
        mode='poke',
        check_existence=True,
        allowed_states=['success'],
        failed_states=['failed', 'upstream_failed'],
    )
    
    run_sys_kill_all_session_pg = TriggerDagRunOperator(
        task_id='run_sys_kill_all_session_pg',
        trigger_dag_id='sys_kill_all_session_pg',
        wait_for_completion=True,
        reset_dag_run=True,
    )
    
    run_wf_data_preparation_for_reports = TriggerDagRunOperator(
        task_id='run_wf_data_preparation_for_reports',
        trigger_dag_id='wf_data_preparation_for_reports',
        wait_for_completion=True,
        reset_dag_run=True,
    )
    
    with TaskGroup("segmentation_group") as segmentation_group:
        load_ds_client_segmentation = TriggerDagRunOperator(
            task_id='load_ds_client_segmentation',
            trigger_dag_id='l1_to_l2_p_load_data_ds_client_segmentation_full',
            wait_for_completion=True,
            reset_dag_run=True,
        )
        
        send_flg_to_sap = PythonOperator(
            task_id='send_flg_to_sap',
            python_callable=send_flag_to_sap,
        )
        
        load_ds_client_segmentation >> send_flg_to_sap
    
    end = PythonOperator(
        task_id='end',
        python_callable=update_workflow_session_status,
    )
    
    wait_for_l2_full_load >> get_load_id >> workflow_registration >> wait_for_success_end >> run_sys_kill_all_session_pg
    run_sys_kill_all_session_pg >> [run_wf_data_preparation_for_reports, load_ds_client_segmentation]
    [run_wf_data_preparation_for_reports, send_flg_to_sap] >> end