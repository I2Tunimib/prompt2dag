from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.sql import SqlSensor
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import pendulum

# Define the timezone
local_tz = pendulum.timezone("Asia/Tashkent")

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['data-team@company.com'],
    'email_on_failure': False,  # Disabled because we use explicit EmailOperator
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='dwh_l2_to_l2_main_workflow',
    description='Main workflow for loading datasets from DWH L2 to L2 with segmentation processing and SAP integration',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=20,
    default_args=default_args,
    tags=['dwh', 'l2', 'segmentation', 'sap'],
) as dag:

    # Task 1: Wait for L1 to L2 data load completion
    wait_for_l2_full_load = SqlSensor(
        task_id='wait_for_l2_full_load',
        conn_id='postgres_dwh',
        sql="""
            SELECT load_status 
            FROM dwh_metadata.load_control 
            WHERE process_name = 'l1_to_l2_full_load' 
            AND execution_date = CURRENT_DATE 
            AND load_status = 'SUCCESS'
        """,
        poke_interval=60,
        timeout=3600,
        mode='poke',
    )

    # Task 2: Get unique load identifier
    def get_load_id(**context):
        """Generate a unique load identifier for session tracking."""
        execution_date = context['execution_date']
        load_id = f"L2_LOAD_{execution_date.strftime('%Y%m%d_%H%M%S')}_{context['run_id']}"
        context['task_instance'].xcom_push(key='load_id', value=load_id)
        return load_id

    get_load_id_task = PythonOperator(
        task_id='get_load_id',
        python_callable=get_load_id,
    )

    # Task 3: Register workflow session
    def register_workflow_session(**context):
        """Register workflow session in metadata tables."""
        # In a real implementation, this would use the md_dwh module
        # from md_dwh import workflow_registration
        # workflow_registration.register_session(context)
        
        load_id = context['task_instance'].xcom_pull(task_ids='get_load_id', key='load_id')
        print(f"Registered workflow session with load_id: {load_id}")
        # Simulate registration
        return True

    workflow_registration = PythonOperator(
        task_id='workflow_registration',
        python_callable=register_workflow_session,
    )

    # Task 4: Wait for previous day's execution to complete
    wait_for_success_end = ExternalTaskSensor(
        task_id='wait_for_success_end',
        external_dag_id='dwh_l2_to_l2_main_workflow',
        external_task_id='end',
        execution_delta=timedelta(days=1),
        poke_interval=60,
        timeout=7200,
        mode='poke',
    )

    # Task 5: Trigger PostgreSQL session kill DAG
    run_sys_kill_all_session_pg = TriggerDagRunOperator(
        task_id='run_sys_kill_all_session_pg',
        trigger_dag_id='sys_kill_all_session_pg',
        wait_for_completion=True,
        poke_interval=60,
        reset_dag_run=True,
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
    )

    # Parallel branch 1: Data preparation for reports
    run_wf_data_preparation_for_reports = TriggerDagRunOperator(
        task_id='run_wf_data_preparation_for_reports',
        trigger_dag_id='wf_data_preparation_for_reports',
        wait_for_completion=True,
        poke_interval=60,
        reset_dag_run=True,
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
    )

    # Parallel branch 2: Segmentation group
    with TaskGroup('segmentation_group') as segmentation_group:

        # Task 6.1: Trigger client segmentation data loading
        load_ds_client_segmentation = TriggerDagRunOperator(
            task_id='load_ds_client_segmentation',
            trigger_dag_id='l1_to_l2_p_load_data_ds_client_segmentation_full',
            wait_for_completion=True,
            poke_interval=60,
            reset_dag_run=True,
            allowed_states=['success'],
            failed_states=['failed', 'skipped'],
        )

        # Task 6.2: Send completion flag to SAP
        def send_flag_to_sap(**context):
            """Send completion flag to SAP system with row count."""
            # In a real implementation, this would:
            # 1. Query row count from segmentation results
            # 2. Send HTTP request to SAP endpoint
            # 3. Handle authentication and error handling
            
            # Simulate getting row count
            row_count = 15000  # This would come from actual query
            
            # Simulate SAP integration
            print(f"Sending completion flag to SAP: row_count={row_count}")
            
            # Simulate HTTP call to SAP
            # import requests
            # response = requests.post(
            #     'https://sap-api.company.com/segmentation/complete',
            #     json={'load_id': load_id, 'row_count': row_count, 'status': 'SUCCESS'},
            #     auth=('user', 'pass')
            # )
            # response.raise_for_status()
            
            return f"SAP notification sent: {row_count} rows"

        send_flg_to_sap = PythonOperator(
            task_id='send_flg_to_sap',
            python_callable=send_flag_to_sap,
        )

        # Define dependencies within TaskGroup
        load_ds_client_segmentation >> send_flg_to_sap

    # Task 7: Mark workflow session as completed
    def mark_workflow_complete(**context):
        """Update metadata tables to mark workflow session as successfully completed."""
        # In a real implementation, this would use the md_dwh module
        # from md_dwh import workflow_completion
        # workflow_completion.update_status(context)
        
        load_id = context['task_instance'].xcom_pull(task_ids='get_load_id', key='load_id')
        print(f"Marked workflow session as completed: {load_id}")
        return True

    end = PythonOperator(
        task_id='end',
        python_callable=mark_workflow_complete,
        trigger_rule='all_success',
    )

    # Task 8: Send failure notification email
    email_on_failure = EmailOperator(
        task_id='email_on_failure',
        to=['data-team@company.com', 'sap-integration@company.com'],
        subject='DWH L2 to L2 Workflow Failed: {{ ds }}',
        html_content="""
        <h3>DWH L2 to L2 Main Workflow Failure</h3>
        <p><strong>Execution Date:</strong> {{ execution_date }}</p>
        <p><strong>DAG ID:</strong> {{ dag.dag_id }}</p>
        <p><strong>Run ID:</strong> {{ run_id }}</p>
        <p>One or more tasks have failed in the workflow. Please check the Airflow UI for details.</p>
        <p><a href="{{ ti.log_url }}">View Logs</a></p>
        """,
        trigger_rule='one_failed',
    )

    # Define main workflow dependencies
    wait_for_l2_full_load >> get_load_id_task >> workflow_registration >> wait_for_success_end >> run_sys_kill_all_session_pg
    run_sys_kill_all_session_pg >> [run_wf_data_preparation_for_reports, segmentation_group]
    [run_wf_data_preparation_for_reports, segmentation_group] >> end

    # Define failure notification dependency
    # The email task depends on all main tasks to ensure it can detect any failure
    wait_for_l2_full_load >> email_on_failure
    get_load_id_task >> email_on_failure
    workflow_registration >> email_on_failure
    wait_for_success_end >> email_on_failure
    run_sys_kill_all_session_pg >> email_on_failure
    run_wf_data_preparation_for_reports >> email_on_failure
    load_ds_client_segmentation >> email_on_failure
    send_flg_to_sap >> email_on_failure
    end >> email_on_failure