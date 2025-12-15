from prefect import flow, task
from prefect.tasks import task_input_hash
import time
from datetime import datetime, timedelta
import pytz
from typing import Optional, Dict, Any
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Simulated modules (these would be actual imports in production)
# import md_dwh
# from prefect.client import get_client

# Configuration
TASHKENT_TZ = pytz.timezone('Asia/Tashkent')
POLLING_INTERVAL = 60  # seconds
MAX_ACTIVE_RUNS = 20
# Email configuration (use environment variables in production)
EMAIL_CONFIG = {
    'smtp_server': 'smtp.example.com',
    'smtp_port': 587,
    'sender': 'prefect@example.com',
    'recipients': ['data-team@example.com'],
    'username': None,
    'password': None
}

# Task implementations
@task
def wait_for_l2_full_load(polling_interval: int = POLLING_INTERVAL) -> bool:
    """Polls metadata table until L1 to L2 load completion flag is set."""
    # Implementation would query PostgreSQL metadata table
    # For demonstration, using a simulated check
    while True:
        # Simulated query: SELECT load_completed FROM metadata.l2_load_status WHERE date = CURRENT_DATE
        load_completed = _simulate_dwh_query()  # Replace with actual DB query
        if load_completed:
            return True
        time.sleep(polling_interval)

@task
def get_load_id() -> str:
    """Generates or retrieves a unique load identifier for session tracking."""
    # Would use md_dwh module or DB sequence
    return f"load_{datetime.now(TASHKENT_TZ).strftime('%Y%m%d_%H%M%S')}"

@task
def workflow_registration(load_id: str) -> Dict[str, Any]:
    """Registers the workflow session in metadata tables."""
    # Would call md_dwh.register_workflow_session()
    return {"session_id": load_id, "status": "started", "timestamp": datetime.now(TASHKENT_TZ)}

@task
def wait_for_success_end(flow_name: str = "dwh_l2_to_l2_segmentation_pipeline") -> bool:
    """Waits for previous day's successful execution using Prefect API."""
    # Would query Prefect API for previous run
    # For demonstration, simulate check
    yesterday = datetime.now(TASHKENT_TZ) - timedelta(days=1)
    # Simulated API call to check if flow ran successfully yesterday
    return _simulate_previous_run_check(flow_name, yesterday)

@task
def run_sys_kill_all_session_pg() -> str:
    """Triggers the system utility DAG to kill all PostgreSQL sessions."""
    # Would use run_deployment or call another flow
    return "Triggered sys_kill_all_session_pg flow"

@task
def run_wf_data_preparation_for_reports() -> str:
    """Launches data preparation workflow for reports."""
    # Would trigger another Prefect flow
    return "Triggered wf_data_preparation_for_reports flow"

@task
def load_ds_client_segmentation() -> Dict[str, Any]:
    """Triggers client segmentation data loading workflow."""
    # Would trigger another flow and return results
    return {"status": "completed", "row_count": 15000}  # Simulated

@task
def send_flg_to_sap(segmentation_result: Dict[str, Any]) -> bool:
    """Sends completion flag to SAP system with row count from segmentation results."""
    row_count = segmentation_result.get("row_count", 0)
    # Would make HTTP call to SAP
    return _simulate_sap_notification(row_count)

@task
def end_workflow_session(load_id: str) -> bool:
    """Updates metadata tables to mark the workflow session as successfully completed."""
    # Would update md_dwh metadata tables
    return True

@task
def email_on_failure(error_message: str) -> None:
    """Sends failure notification email."""
    msg = MIMEMultipart()
    msg['From'] = EMAIL_CONFIG['sender']
    msg['To'] = ', '.join(EMAIL_CONFIG['recipients'])
    msg['Subject'] = f"Prefect Flow Failure: DWH L2 to L2 Pipeline"
    body = f"The pipeline failed with error: {error_message}"
    msg.attach(MIMEText(body, 'plain'))
    
    # Would send via SMTP
    # server = smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port'])
    # server.send_message(msg)
    # server.quit()
    print(f"FAILURE EMAIL: {body}")  # Simulated

# Helper functions (simulated)
def _simulate_dwh_query() -> bool:
    """Simulates checking DWH metadata table."""
    # In production: actual PostgreSQL query
    return True  # Simulated success

def _simulate_previous_run_check(flow_name: str, target_date: datetime) -> bool:
    """Simulates checking previous flow run status."""
    # In production: Query Prefect API
    return True  # Simulated success

def _simulate_sap_notification(row_count: int) -> bool:
    """Simulates sending notification to SAP."""
    # In production: HTTP POST to SAP endpoint
    print(f"SAP notification sent: {row_count} rows processed")
    return True

# Main flow
@flow(name="dwh-l2-to-l2-segmentation-pipeline")
def dwh_l2_to_l2_segmentation_pipeline():
    """
    Main workflow for loading datasets from DWH L2 to L2 with segmentation processing and SAP integration.
    """
    try:
        # Sequential steps
        # 1. Wait for L1 to L2 load completion
        l2_load_complete = wait_for_l2_full_load()
        
        # 2. Get load ID
        load_id = get_load_id()
        
        # 3. Register workflow session
        session_info = workflow_registration(load_id)
        
        # 4. Wait for previous day's execution
        previous_run_complete = wait_for_success_end()
        
        # 5. Kill PostgreSQL sessions
        kill_sessions_result = run_sys_kill_all_session_pg()
        
        # Parallel branches
        # Branch 1: Data preparation for reports
        data_prep_future = run_wf_data_preparation_for_reports.submit()
        
        # Branch 2: Segmentation group
        segmentation_future = load_ds_client_segmentation.submit()
        
        # Wait for segmentation to complete before sending SAP flag
        segmentation_result = segmentation_future.result()
        sap_notification_future = send_flg_to_sap.submit(segmentation_result)
        
        # Wait for all parallel tasks to complete
        data_prep_future.result()
        sap_notification_future.result()
        
        # Final step: Mark workflow as completed
        end_workflow_session(load_id)
        
    except Exception as e:
        # Send failure notification
        email_on_failure(str(e))
        raise  # Re-raise to mark flow as failed

if __name__ == '__main__':
    # Manual trigger only - no schedule
    # For deployment with schedule, use:
    # dwh_l2_to_l2_segmentation_pipeline.serve(
    #     name="dwh-l2-to-l2-pipeline",
    #     schedule=None,  # Manual trigger only
    #     parameters={}
    # )
    dwh_l2_to_l2_segmentation_pipeline()