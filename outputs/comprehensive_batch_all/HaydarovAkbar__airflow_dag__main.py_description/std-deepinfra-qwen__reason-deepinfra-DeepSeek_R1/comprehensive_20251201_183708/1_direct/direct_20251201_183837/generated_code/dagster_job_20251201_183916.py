from dagster import op, job, resource, RetryPolicy, Failure, success_hook, failure_hook
from dagster.utils import file_relative_path
from datetime import timedelta
import pendulum

# Resources
@resource
def metadata_db_resource():
    # Simplified metadata DB resource
    pass

@resource
def sap_system_resource():
    # Simplified SAP system resource
    pass

@resource
def smtp_resource():
    # Simplified SMTP resource for email notifications
    pass

# Ops
@op(required_resource_keys={"metadata_db"})
def wait_for_l2_full_load(context):
    """SqlSensor to monitor L1 to L2 data load completion."""
    # Simulate checking a flag in the metadata table
    context.log.info("Waiting for L1 to L2 data load completion...")
    # Placeholder for actual sensor logic
    return True

@op
def get_load_id(context):
    """Retrieves a unique load identifier for session tracking."""
    # Simulate retrieving a load ID
    load_id = "12345"
    context.log.info(f"Retrieved load ID: {load_id}")
    return load_id

@op(required_resource_keys={"metadata_db"})
def workflow_registration(context, load_id):
    """Registers the workflow session in metadata tables and initiates logging."""
    context.log.info(f"Registering workflow session with load ID: {load_id}")
    # Placeholder for actual registration logic
    return load_id

@op
def wait_for_success_end(context):
    """ExternalTaskSensor to wait for the previous day's execution to complete successfully."""
    context.log.info("Waiting for the previous day's execution to complete...")
    # Placeholder for actual sensor logic
    return True

@op
def run_sys_kill_all_session_pg(context):
    """TriggerDagRunOperator to trigger a system utility DAG to kill all PostgreSQL sessions."""
    context.log.info("Triggering system utility DAG to kill all PostgreSQL sessions...")
    # Placeholder for actual trigger logic
    return True

@op
def run_wf_data_preparation_for_reports(context):
    """TriggerDagRunOperator to launch data preparation workflow for reports."""
    context.log.info("Launching data preparation workflow for reports...")
    # Placeholder for actual trigger logic
    return True

@op(required_resource_keys={"metadata_db"})
def load_ds_client_segmentation(context):
    """TriggerDagRunOperator to trigger client segmentation data loading workflow."""
    context.log.info("Triggering client segmentation data loading workflow...")
    # Placeholder for actual trigger logic
    return True

@op(required_resource_keys={"sap_system"})
def send_flg_to_sap(context, row_count):
    """PythonOperator to send completion flag to SAP system with row count from segmentation results."""
    context.log.info(f"Sending completion flag to SAP with row count: {row_count}")
    # Placeholder for actual SAP integration logic
    return True

@op(required_resource_keys={"metadata_db"})
def end(context, load_id):
    """PythonOperator to update metadata tables to mark the workflow session as successfully completed."""
    context.log.info(f"Marking workflow session with load ID {load_id} as completed.")
    # Placeholder for actual update logic
    return True

@failure_hook(required_resource_keys={"smtp"})
def email_on_failure(context):
    """EmailOperator to send failure notification if any upstream task fails."""
    context.log.error("Pipeline failed. Sending failure notification...")
    # Placeholder for actual email sending logic
    return True

# Job
@job(
    resource_defs={
        "metadata_db": metadata_db_resource,
        "sap_system": sap_system_resource,
        "smtp": smtp_resource,
    },
    tags={
        "pipeline": "L2_data_processing",
        "timezone": "Asia/Tashkent",
    },
    description="Main workflow for loading datasets from DWH L2 to L2 with segmentation processing and SAP integration.",
    retry_policy=RetryPolicy(max_retries=3, delay=60),
    op_retry_policy=RetryPolicy(max_retries=3, delay=60),
    max_concurrent_runs=20,
    schedule_interval=None,
    execution_timezone="Asia/Tashkent",
    on_failure=email_on_failure,
)
def l2_data_processing_job():
    l2_full_load = wait_for_l2_full_load()
    load_id = get_load_id(l2_full_load)
    registered_load_id = workflow_registration(load_id)
    success_end = wait_for_success_end(registered_load_id)
    sys_kill_all_session_pg = run_sys_kill_all_session_pg(success_end)
    
    data_preparation = run_wf_data_preparation_for_reports(sys_kill_all_session_pg)
    
    client_segmentation = load_ds_client_segmentation(sys_kill_all_session_pg)
    send_flg_to_sap(client_segmentation)
    
    end(sys_kill_all_session_pg)

if __name__ == "__main__":
    result = l2_data_processing_job.execute_in_process()