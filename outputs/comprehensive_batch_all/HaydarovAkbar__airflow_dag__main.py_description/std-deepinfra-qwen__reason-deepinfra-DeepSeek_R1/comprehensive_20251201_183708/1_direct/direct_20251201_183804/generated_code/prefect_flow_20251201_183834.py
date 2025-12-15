from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
import time
from prefect_email import EmailServerCredentials, email_send_message
from prefect.blocks.system import Secret
from prefect.blocks.notifications import EmailNotificationBlock

# Mock imports for external dependencies
from md_dwh import register_workflow, update_workflow_status, get_load_id
from sql_sensor import SqlSensor
from external_task_sensor import ExternalTaskSensor
from trigger_dag_run import TriggerDagRunOperator
from sap_integration import send_completion_flag_to_sap

# Constants
EMAIL_RECIPIENTS = ["admin@example.com"]
SQL_SENSOR_INTERVAL = 60
EXTERNAL_TASK_SENSOR_INTERVAL = 60
TIMEZONE = "Asia/Tashkent"

# Tasks
@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def wait_for_l2_full_load():
    logger = get_run_logger()
    logger.info("Waiting for L1 to L2 data load completion...")
    SqlSensor.poll_for_completion(interval=SQL_SENSOR_INTERVAL, timezone=TIMEZONE)
    logger.info("L1 to L2 data load completed.")

@task
def get_load_id():
    logger = get_run_logger()
    logger.info("Retrieving unique load identifier...")
    load_id = get_load_id()
    logger.info(f"Load ID: {load_id}")
    return load_id

@task
def workflow_registration(load_id):
    logger = get_run_logger()
    logger.info("Registering workflow session...")
    register_workflow(load_id)
    logger.info("Workflow session registered.")

@task
def wait_for_success_end():
    logger = get_run_logger()
    logger.info("Waiting for previous day's execution to complete...")
    ExternalTaskSensor.poll_for_completion(interval=EXTERNAL_TASK_SENSOR_INTERVAL, timezone=TIMEZONE)
    logger.info("Previous day's execution completed.")

@task
def run_sys_kill_all_session_pg():
    logger = get_run_logger()
    logger.info("Triggering system utility DAG to kill all PostgreSQL sessions...")
    TriggerDagRunOperator.trigger_dag("sys_kill_all_session_pg")
    logger.info("PostgreSQL sessions killed.")

@task
def run_wf_data_preparation_for_reports():
    logger = get_run_logger()
    logger.info("Launching data preparation workflow for reports...")
    TriggerDagRunOperator.trigger_dag("wf_data_preparation_for_reports")
    logger.info("Data preparation workflow for reports launched.")

@task
def load_ds_client_segmentation():
    logger = get_run_logger()
    logger.info("Triggering client segmentation data loading workflow...")
    TriggerDagRunOperator.trigger_dag("l1_to_l2_p_load_data_ds_client_segmentation_full")
    logger.info("Client segmentation data loading workflow triggered.")

@task
def send_flg_to_sap(row_count):
    logger = get_run_logger()
    logger.info("Sending completion flag to SAP system...")
    send_completion_flag_to_sap(row_count)
    logger.info("Completion flag sent to SAP system.")

@task
def end(load_id):
    logger = get_run_logger()
    logger.info("Updating metadata tables to mark workflow session as completed...")
    update_workflow_status(load_id, "completed")
    logger.info("Workflow session marked as completed.")

@task
def email_on_failure(error_message):
    logger = get_run_logger()
    logger.info("Sending failure notification...")
    email_send_message(
        email_server_credentials=EmailServerCredentials.load("email-credentials"),
        subject="Pipeline Failure Notification",
        msg=f"Pipeline failed with error: {error_message}",
        email_to=EMAIL_RECIPIENTS,
    )
    logger.info("Failure notification sent.")

# Flow
@flow(name="L2 Data Processing and SAP Integration")
def l2_data_processing_flow():
    try:
        wait_for_l2_full_load()
        load_id = get_load_id()
        workflow_registration(load_id)
        wait_for_success_end()
        run_sys_kill_all_session_pg()

        # Parallel execution
        data_prep_future = run_wf_data_preparation_for_reports.submit()
        segmentation_future = load_ds_client_segmentation.submit()

        # Wait for parallel tasks to complete
        data_prep_future.result()
        row_count = segmentation_future.result()

        send_flg_to_sap(row_count)
        end(load_id)
    except Exception as e:
        email_on_failure(str(e))
        raise

if __name__ == "__main__":
    l2_data_processing_flow()