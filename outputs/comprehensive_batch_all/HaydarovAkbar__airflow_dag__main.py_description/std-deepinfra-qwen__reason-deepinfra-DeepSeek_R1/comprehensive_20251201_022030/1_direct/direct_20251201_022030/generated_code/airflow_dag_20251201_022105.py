from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.sql import SqlSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from datetime import timedelta, timezone
import md_dwh  # Custom module for metadata operations

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['admin@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 20,
    'timezone': timezone(timedelta(hours=5))  # Asia/Tashkent
}

with DAG(
    'l2_data_processing_pipeline',
    default_args=default_args,
    description='Main workflow for loading datasets from DWH L2 to L2 with segmentation processing and SAP integration',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['data_processing', 'segmentation', 'sap_integration']
) as dag:

    def get_load_id():
        """Retrieve a unique load identifier for session tracking."""
        return md_dwh.get_unique_load_id()

    def workflow_registration(load_id):
        """Register the workflow session in metadata tables and initiate logging."""
        md_dwh.register_workflow_session(load_id)

    def send_flg_to_sap(row_count):
        """Send completion flag to SAP system with row count from segmentation results."""
        md_dwh.send_sap_completion_flag(row_count)

    def end_workflow(load_id):
        """Update metadata tables to mark the workflow session as successfully completed."""
        md_dwh.mark_workflow_completed(load_id)

    wait_for_l2_full_load = SqlSensor(
        task_id='wait_for_l2_full_load',
        conn_id='dwh_postgres_conn',
        sql="SELECT * FROM metadata_table WHERE load_status = 'completed'",
        mode='poke',
        poke_interval=60,
        timeout=100
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
        mode='poke',
        poke_interval=60,
        timeout=100
    )

    run_sys_kill_all_session_pg = TriggerDagRunOperator(
        task_id='run_sys_kill_all_session_pg',
        trigger_dag_id='sys_kill_all_session_pg'
    )

    with TaskGroup(group_id='segmentation_group') as segmentation_group:
        load_ds_client_segmentation = TriggerDagRunOperator(
            task_id='load_ds_client_segmentation',
            trigger_dag_id='l1_to_l2_p_load_data_ds_client_segmentation_full'
        )

        def send_flg_to_sap_task():
            row_count = md_dwh.get_segmentation_row_count()
            send_flg_to_sap(row_count)

        send_flg_to_sap_task = PythonOperator(
            task_id='send_flg_to_sap',
            python_callable=send_flg_to_sap_task
        )

        load_ds_client_segmentation >> send_flg_to_sap_task

    run_wf_data_preparation_for_reports = TriggerDagRunOperator(
        task_id='run_wf_data_preparation_for_reports',
        trigger_dag_id='wf_data_preparation_for_reports'
    )

    end_task = PythonOperator(
        task_id='end',
        python_callable=end_workflow,
        op_kwargs={'load_id': '{{ ti.xcom_pull(task_ids="get_load_id") }}'}
    )

    email_on_failure = EmailOperator(
        task_id='email_on_failure',
        to='admin@example.com',
        subject='Pipeline Failure',
        html_content='The pipeline has failed. Please check the logs for more details.'
    )

    wait_for_l2_full_load >> get_load_id_task >> workflow_registration_task >> wait_for_success_end >> run_sys_kill_all_session_pg
    run_sys_kill_all_session_pg >> [run_wf_data_preparation_for_reports, segmentation_group] >> end_task
    end_task >> email_on_failure