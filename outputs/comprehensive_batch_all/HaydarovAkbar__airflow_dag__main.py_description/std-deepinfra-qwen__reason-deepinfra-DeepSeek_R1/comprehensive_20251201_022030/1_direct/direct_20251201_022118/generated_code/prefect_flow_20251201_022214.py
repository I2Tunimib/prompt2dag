from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
import time

# Mock imports for demonstration purposes
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect_email import EmailSender
from prefect_sensors import SqlSensor, ExternalTaskSensor
from prefect_triggers import TriggerDagRun

# Constants
SQL_SENSOR_POLL_INTERVAL = 60
EXTERNAL_TASK_SENSOR_DELTA = timedelta(hours=24)
TIMEZONE = "Asia/Tashkent"
EMAIL_RECIPIENTS = ["admin@example.com"]

# Task definitions
@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def wait_for_l2_full_load():
    """SqlSensor monitors for successful L1 to L2 data load completion."""
    with SqlAlchemyConnector.load("dwh-connection") as engine:
        query = "SELECT * FROM metadata_table WHERE load_status = 'success'"
        sensor = SqlSensor(
            query=query,
            poll_interval=SQL_SENSOR_POLL_INTERVAL,
            timeout=100,
            engine=engine
        )
        sensor.run()

@task
def get_load_id():
    """PythonOperator retrieves a unique load identifier for session tracking."""
    # Mock load ID retrieval
    return "1234567890"

@task
def workflow_registration(load_id: str):
    """PythonOperator registers the workflow session in metadata tables and initiates logging."""
    # Mock workflow registration
    print(f"Workflow registered with load ID: {load_id}")

@task
def wait_for_success_end():
    """ExternalTaskSensor waits for the previous day's execution of this same DAG to complete successfully."""
    sensor = ExternalTaskSensor(
        external_dag_id="this_dag",
        external_task_id="end",
        execution_delta=EXTERNAL_TASK_SENSOR_DELTA,
        timeout=100,
        poke_interval=SQL_SENSOR_POLL_INTERVAL,
        mode="reschedule",
        timezone=TIMEZONE
    )
    sensor.run()

@task
def run_sys_kill_all_session_pg():
    """TriggerDagRunOperator triggers a system utility DAG to kill all PostgreSQL sessions."""
    trigger = TriggerDagRun(
        trigger_dag_id="sys_kill_all_session_pg",
        conf={"load_id": "1234567890"}
    )
    trigger.run()

@task
def run_wf_data_preparation_for_reports():
    """TriggerDagRunOperator launches data preparation workflow for reports."""
    trigger = TriggerDagRun(
        trigger_dag_id="wf_data_preparation_for_reports",
        conf={"load_id": "1234567890"}
    )
    trigger.run()

@task
def load_ds_client_segmentation():
    """TriggerDagRunOperator triggers client segmentation data loading workflow."""
    trigger = TriggerDagRun(
        trigger_dag_id="l1_to_l2_p_load_data_ds_client_segmentation_full",
        conf={"load_id": "1234567890"}
    )
    trigger.run()

@task
def send_flg_to_sap(row_count: int):
    """PythonOperator sends completion flag to SAP system with row count from segmentation results."""
    # Mock SAP notification
    print(f"Sent completion flag to SAP with row count: {row_count}")

@task
def end(load_id: str):
    """PythonOperator updates metadata tables to mark the workflow session as successfully completed."""
    # Mock end of workflow
    print(f"Workflow session {load_id} marked as completed")

@task
def email_on_failure(failure_message: str):
    """EmailOperator sends failure notification if any upstream task fails."""
    email_sender = EmailSender(email_server_credentials="smtp-credentials")
    email_sender.send_email(
        subject="Pipeline Failure Notification",
        recipients=EMAIL_RECIPIENTS,
        message=f"Pipeline failed with message: {failure_message}"
    )

# Flow definition
@flow(name="L2 Data Processing Pipeline")
def l2_data_processing_pipeline():
    try:
        wait_for_l2_full_load()
        load_id = get_load_id()
        workflow_registration(load_id)
        wait_for_success_end()
        run_sys_kill_all_session_pg()

        # Parallel execution
        data_prep_future = run_wf_data_preparation_for_reports.submit()
        segmentation_future = load_ds_client_segmentation.submit()

        # Wait for segmentation to complete
        row_count = segmentation_future.result()
        send_flg_to_sap(row_count)

        # Wait for data preparation to complete
        data_prep_future.result()

        end(load_id)
    except Exception as e:
        email_on_failure(str(e))

if __name__ == "__main__":
    l2_data_processing_pipeline()