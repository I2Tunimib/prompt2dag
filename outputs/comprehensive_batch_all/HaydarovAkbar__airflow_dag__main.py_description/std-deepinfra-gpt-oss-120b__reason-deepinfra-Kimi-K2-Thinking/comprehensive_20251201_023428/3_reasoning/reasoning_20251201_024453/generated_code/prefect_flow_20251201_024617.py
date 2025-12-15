from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
import time
import logging
import uuid
from typing import Dict, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CONFIG = {
    "database_connection": "postgresql://user:pass@host:5432/dwh",
    "sap_endpoint": "https://sap.example.com/api/flag",
    "email_recipients": ["data-team@example.com"],
    "timezone": "Asia/Tashkent",
    "max_active_runs": 20,
    "sensor_polling_interval": 60,
    "external_task_sensor_delta": 86400,
}


@task(retries=3, retry_delay_seconds=60)
def wait_for_l2_full_load_task() -> bool:
    """Simulates SqlSensor monitoring L1 to L2 data load completion."""
    logger.info("Polling for L1 to L2 full load completion flag...")
    time.sleep(CONFIG["sensor_polling_interval"])
    return True


@task
def get_load_id_task() -> str:
    """Generates a unique load identifier for session tracking."""
    load_id = str(uuid.uuid4())
    logger.info(f"Generated load ID: {load_id}")
    return load_id


@task
def workflow_registration_task(load_id: str) -> Dict[str, Any]:
    """Registers workflow session in metadata tables."""
    logger.info(f"Registering workflow session: {load_id}")
    return {"load_id": load_id, "status": "started"}


@task(retries=2, retry_delay_seconds=100)
def wait_for_success_end_task() -> bool:
    """Simulates ExternalTaskSensor for previous day's execution."""
    logger.info("Waiting for previous day's execution...")
    time.sleep(CONFIG["sensor_polling_interval"])
    return True


@task
def run_sys_kill_all_session_pg_task() -> bool:
    """Triggers PostgreSQL session kill utility."""
    logger.info("Triggering sys_kill_all_session_pg workflow...")
    return True


@task
def run_wf_data_preparation_for_reports_task(load_id: str) -> bool:
    """Triggers data preparation workflow for reports."""
    logger.info(f"Triggering data prep workflow: {load_id}")
    return True


@task
def load_ds_client_segmentation_task(load_id: str) -> int:
    """Triggers client segmentation data loading workflow."""
    logger.info(f"Triggering segmentation workflow: {load_id}")
    return 1500


@task
def send_flg_to_sap_task(row_count: int, load_id: str) -> bool:
    """Sends completion flag to SAP system with row count."""
    logger.info(f"Sending SAP flag: load_id={load_id}, row_count={row_count}")
    return True


@task
def end_task(load_id: str) -> bool:
    """Updates metadata tables to mark session as completed."""
    logger.info(f"Marking workflow complete: {load_id}")
    return True


@task
def email_on_failure_task(error_message: str) -> bool:
    """Sends failure notification email."""
    logger.error(f"Pipeline failure: {error_message}")
    recipients = CONFIG["email_recipients"]
    print(f"EMAIL ALERT to {recipients}: {error_message}")
    return True


@flow(
    name="dwh-l2-to-l2-segmentation",
    description="DWH L2 to L2 pipeline with segmentation and SAP integration",
    task_runner=ConcurrentTaskRunner(),
    # Deployment: manual trigger, no catchup, concurrency_limit=20, timezone=Asia/Tashkent
)
def dwh_l2_to_l2_segmentation_flow():
    """Main orchestration flow."""
    load_id = None
    
    try:
        wait_for_l2_full_load_task.submit().result()
        load_id = get_load_id_task.submit().result()
        workflow_registration_task.submit(load_id).result()
        wait_for_success_end_task.submit().result()
        run_sys_kill_all_session_pg_task.submit().result()
        
        # Parallel branches
        data_prep = run_wf_data_preparation_for_reports_task.submit(load_id)
        segmentation = load_ds_client_segmentation_task.submit(load_id)
        row_count = segmentation.result()
        sap_flag = send_flg_to_sap_task.submit(row_count, load_id)
        
        data_prep.result()
        sap_flag.result()
        
        end_task.submit(load_id).result()
        logger.info("Pipeline completed successfully")
        
    except Exception as exc:
        error_msg = f"DWH L2 pipeline failed: {str(exc)}"
        email_on_failure_task.submit(error_msg)
        raise


if __name__ == "__main__":
    dwh_l2_to_l2_segmentation_flow()