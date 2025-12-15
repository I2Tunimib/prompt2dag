from dagster import op, job, resource, RetryPolicy, Failure, success_hook, failure_hook
from dagster.utils import file_relative_path
from datetime import timedelta
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.sql_sensor import SqlSensor
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow import DAG
import pendulum

# Resources
@resource
def dwh_metadata_resource():
    # Simplified resource for DWH metadata operations
    pass

@resource
def sap_integration_resource():
    # Simplified resource for SAP integration
    pass

@resource
def smtp_resource():
    # Simplified resource for SMTP integration
    pass

# Ops
@op(required_resource_keys={"dwh_metadata_resource"})
def wait_for_l2_full_load(context):
    # SqlSensor to monitor L1 to L2 data load completion
    pass

@op(required_resource_keys={"dwh_metadata_resource"})
def get_load_id(context):
    # Retrieves a unique load identifier for session tracking
    return "unique_load_id"

@op(required_resource_keys={"dwh_metadata_resource"})
def workflow_registration(context, load_id):
    # Registers the workflow session in metadata tables and initiates logging
    pass

@op
def wait_for_success_end(context):
    # ExternalTaskSensor to wait for the previous day's execution to complete successfully
    pass

@op
def run_sys_kill_all_session_pg(context):
    # TriggerDagRunOperator to trigger a system utility DAG to kill all PostgreSQL sessions
    pass

@op
def run_wf_data_preparation_for_reports(context):
    # TriggerDagRunOperator to launch data preparation workflow for reports
    pass

@op(required_resource_keys={"dwh_metadata_resource"})
def load_ds_client_segmentation(context):
    # TriggerDagRunOperator to trigger client segmentation data loading workflow
    pass

@op(required_resource_keys={"sap_integration_resource"})
def send_flg_to_sap(context, row_count):
    # Sends completion flag to SAP system with row count from segmentation results
    pass

@op(required_resource_keys={"dwh_metadata_resource"})
def end(context):
    # Updates metadata tables to mark the workflow session as successfully completed
    pass

@op(required_resource_keys={"smtp_resource"})
def email_on_failure(context):
    # Sends failure notification if any upstream task fails
    pass

# Hooks
@success_hook
def on_success(context):
    pass

@failure_hook
def on_failure(context):
    email_on_failure(context)

# Job
@job(
    resource_defs={
        "dwh_metadata_resource": dwh_metadata_resource,
        "sap_integration_resource": sap_integration_resource,
        "smtp_resource": smtp_resource
    },
    tags={"pipeline": "L2_to_L2_with_segmentation"},
    description="Main workflow for loading datasets from DWH L2 to L2 with segmentation processing and SAP integration.",
    hooks=[on_success, on_failure],
    retry_policy=RetryPolicy(max_retries=3, delay=timedelta(seconds=60)),
    executor_defs=None,
    op_retry_policy=RetryPolicy(max_retries=3, delay=timedelta(seconds=60)),
    op_retry_delay=timedelta(seconds=60),
    op_retry_backoff=1.5,
    op_retry_jitter=0.5,
    op_retry_max_jitter=1.0,
    op_retry_delay_factor=1.0,
    op_retry_max_delay=timedelta(minutes=10),
    op_retry_policy=RetryPolicy(max_retries=3, delay=timedelta(seconds=60)),
    op_retry_delay=timedelta(seconds=60),
    op_retry_backoff=1.5,
    op_retry_jitter=0.5,
    op_retry_max_jitter=1.0,
    op_retry_delay_factor=1.0,
    op_retry_max_delay=timedelta(minutes=10),
    op_retry_policy=RetryPolicy(max_retries=3, delay=timedelta(seconds=60)),
    op_retry_delay=timedelta(seconds=60),
    op_retry_backoff=1.5,
    op_retry_jitter=0.5,
    op_retry_max_jitter=1.0,
    op_retry_delay_factor=1.0,
    op_retry_max_delay=timedelta(minutes=10),
)
def l2_to_l2_with_segmentation():
    load_id = get_load_id()
    workflow_registration(load_id)
    wait_for_l2_full_load()
    wait_for_success_end()
    run_sys_kill_all_session_pg()
    
    data_preparation = run_wf_data_preparation_for_reports()
    segmentation_group = load_ds_client_segmentation().then(send_flg_to_sap(row_count=1000))
    
    end()

if __name__ == '__main__':
    result = l2_to_l2_with_segmentation.execute_in_process()