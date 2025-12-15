from prefect import flow, task
import time
import random
from datetime import datetime, timedelta
from typing import Optional, List


@task(retries=3, retry_delay_seconds=60)
def wait_for_l2_full_load(polling_interval: int = 60, max_attempts: int = 10) -> bool:
    """Simulates SQL sensor monitoring L1 to L2 data load completion."""
    for attempt in range(max_attempts):
        print(f"Polling for L2 full load completion (attempt {attempt + 1}/{max_attempts})...")
        load_complete = random.choice([True, False, False])
        if load_complete:
            print("L2 full load completed successfully.")
            return True
        time.sleep(polling_interval)
    raise Exception("L2 full load did not complete within expected time.")


@task
def get_load_id() -> str:
    """Retrieves a unique load identifier for session tracking."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"load_{timestamp}_{random.randint(1000, 9999)}"


@task
def workflow_registration(load_id: str) -> None:
    """Registers the workflow session in metadata tables and initiates logging."""
    print(f"Registering workflow session with load_id: {load_id}")
    time.sleep(2)


@task(retries=2, retry_delay_seconds=100)
def wait_for_success_end(load_id: str, polling_interval: int = 100) -> None:
    """Waits for the previous day's execution of this same flow to complete successfully."""
    print(f"Checking previous day's execution for load_id pattern: {load_id[:13]}...")
    time.sleep(polling_interval)
    print("Previous day's execution completed successfully.")


@task
def run_sys_kill_all_session_pg() -> None:
    """Triggers a system utility flow to kill all PostgreSQL sessions."""
    print("Triggering sys_kill_all_session_pg flow...")
    time.sleep(3)


@task
def run_wf_data_preparation_for_reports() -> str:
    """Launches data preparation workflow for reports."""
    print("Triggering wf_data_preparation_for_reports flow...")
    time.sleep(5)
    return "reports_prepared"


@task
def load_ds_client_segmentation() -> int:
    """Triggers client segmentation data loading workflow."""
    print("Triggering client segmentation data loading...")
    time.sleep(4)
    return random.randint(1000, 10000)


@task
def send_flg_to_sap(row_count: int) -> None:
    """Sends completion flag to SAP system with row count from segmentation results."""
    print(f"Sending completion flag to SAP with row count: {row_count:,}")
    if row_count < 1:
        raise ValueError("No segmentation data loaded")
    time.sleep(2)


@task
def end(load_id: str) -> None:
    """Updates metadata tables to mark the workflow session as successfully completed."""
    print(f"Marking workflow session {load_id} as completed in metadata tables.")
    time.sleep(2)


@task
def email_on_failure(error_message: str, recipients: List[str]) -> None:
    """Sends failure notification email if any upstream task fails."""
    print(f"Sending failure email to {recipients}")
    print(f"Error: {error_message}")
    time.sleep(2)


@flow(
    name="dwh-l2-to-l2-segmentation-sap-integration",
    description="Main workflow for loading datasets from DWH L2 to L2 with segmentation processing and SAP integration.",
)
def dwh_l2_to_l2_segmentation_sap_integration(
    email_recipients: Optional[List[str]] = None,
    polling_interval_l2: int = 60,
    polling_interval_external: int = 100,
):
    """
    Main workflow for loading datasets from DWH L2 to L2 with segmentation processing and SAP integration.
    """
    if email_recipients is None:
        email_recipients = ["data-team@example.com"]
    
    load_id = None
    
    try:
        wait_for_l2_full_load(polling_interval=polling_interval_l2)
        load_id = get_load_id()
        workflow_registration(load_id)
        wait_for_success_end(load_id, polling_interval=polling_interval_external)
        run_sys_kill_all_session_pg()
        
        reports_future = run_wf_data_preparation_for_reports.submit()
        segmentation_future = load_ds_client_segmentation.submit()
        row_count = segmentation_future.result()
        send_flg_to_sap(row_count)
        
        reports_future.result()
        end(load_id)
        
    except Exception as exc:
        error_msg = f"Flow failed: {str(exc)}"
        if load_id:
            error_msg += f" (load_id: {load_id})"
        email_on_failure(error_msg, email_recipients)
        raise


if __name__ == "__main__":
    dwh_l2_to_l2_segmentation_sap_integration()