from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.sql import SqlSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from datetime import timedelta
import pendulum

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['admin@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 20,
}

# Define the DAG
with DAG(
    dag_id='l2_data_processing_pipeline',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=20,
    tags=['data_processing', 'l2', 'segmentation', 'sap'],
    timezone=pendulum.timezone('Asia/Tashkent'),
) as dag:

    # Task to wait for L1 to L2 data load completion
    wait_for_l2_full_load = SqlSensor(
        task_id='wait_for_l2_full_load',
        conn_id='dwh_postgres_conn',
        sql="SELECT * FROM metadata_table WHERE load_status = 'completed';",
        mode='poke',
        poke_interval=60,
        timeout=100,
    )

    # Task to retrieve a unique load identifier
    def get_load_id_func():
        # Custom logic to retrieve a unique load identifier
        return 'load_id_12345'

    get_load_id = PythonOperator(
        task_id='get_load_id',
        python_callable=get_load_id_func,
    )

    # Task to register the workflow session
    def workflow_registration_func(load_id):
        # Custom logic to register the workflow session
        print(f"Workflow session registered with load_id: {load_id}")

    workflow_registration = PythonOperator(
        task_id='workflow_registration',
        python_callable=workflow_registration_func,
        op_kwargs={'load_id': '{{ ti.xcom_pull(task_ids="get_load_id") }}'},
    )

    # Task to wait for the previous day's execution to complete
    wait_for_success_end = ExternalTaskSensor(
        task_id='wait_for_success_end',
        external_dag_id='l2_data_processing_pipeline',
        external_task_id='end',
        execution_delta=timedelta(days=1),
        mode='poke',
        poke_interval=60,
        timeout=100,
    )

    # Task to trigger the system utility DAG to kill all PostgreSQL sessions
    run_sys_kill_all_session_pg = TriggerDagRunOperator(
        task_id='run_sys_kill_all_session_pg',
        trigger_dag_id='sys_kill_all_session_pg',
    )

    # Task to trigger the data preparation workflow for reports
    run_wf_data_preparation_for_reports = TriggerDagRunOperator(
        task_id='run_wf_data_preparation_for_reports',
        trigger_dag_id='wf_data_preparation_for_reports',
    )

    # Task group for client segmentation
    with TaskGroup(group_id='segmentation_group') as segmentation_group:
        # Task to trigger the client segmentation data loading workflow
        load_ds_client_segmentation = TriggerDagRunOperator(
            task_id='load_ds_client_segmentation',
            trigger_dag_id='l1_to_l2_p_load_data_ds_client_segmentation_full',
        )

        # Task to send completion flag to SAP system
        def send_flg_to_sap_func():
            # Custom logic to send completion flag to SAP system
            print("Sending completion flag to SAP system")

        send_flg_to_sap = PythonOperator(
            task_id='send_flg_to_sap',
            python_callable=send_flg_to_sap_func,
        )

        # Set dependencies within the task group
        load_ds_client_segmentation >> send_flg_to_sap

    # Task to mark the workflow session as successfully completed
    def end_func(load_id):
        # Custom logic to update metadata tables
        print(f"Workflow session marked as completed with load_id: {load_id}")

    end = PythonOperator(
        task_id='end',
        python_callable=end_func,
        op_kwargs={'load_id': '{{ ti.xcom_pull(task_ids="get_load_id") }}'},
    )

    # Task to send failure notification
    email_on_failure = EmailOperator(
        task_id='email_on_failure',
        to='admin@example.com',
        subject='Pipeline Failure',
        html_content='The pipeline has failed. Please check the logs for more details.',
        trigger_rule='one_failed',
    )

    # Set the task dependencies
    wait_for_l2_full_load >> get_load_id >> workflow_registration >> wait_for_success_end >> run_sys_kill_all_session_pg
    run_sys_kill_all_session_pg >> [run_wf_data_preparation_for_reports, segmentation_group] >> end
    end >> email_on_failure