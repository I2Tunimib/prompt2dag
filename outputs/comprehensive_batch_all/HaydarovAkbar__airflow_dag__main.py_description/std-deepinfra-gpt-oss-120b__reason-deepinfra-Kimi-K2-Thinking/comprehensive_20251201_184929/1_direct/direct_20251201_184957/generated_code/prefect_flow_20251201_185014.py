import uuid
import time
import logging
import os
from typing import Any

import requests
import smtplib
from email.message import EmailMessage

from prefect import flow, task

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@task(retries=3, retry_delay_seconds=30)
def wait_for_l2_full_load(poll_interval: int = 60, timeout: int = 3600) -> bool:
    """
    Simulate a SQL sensor that waits for a flag indicating successful L1 to L2 load.
    """
    logger.info("Waiting for L2 full load flag...")
    elapsed = 0
    while elapsed < timeout:
        # Placeholder for actual DB check
        flag_set = True  # Replace with real check
        if flag_set:
            logger.info("L2 full load flag detected.")
            return True
        time.sleep(poll_interval)
        elapsed += poll_interval
    raise TimeoutError("Timed out waiting for L2 full load flag.")


@task
def get_load_id() -> str:
    """
    Retrieve a unique load identifier for the session.
    """
    load_id = str(uuid.uuid4())
    logger.info("Generated load_id: %s", load_id)
    return load_id


@task
def workflow_registration(load_id: str) -> None:
    """
    Register the workflow session in metadata tables and initiate logging.
    """
    logger.info("Registering workflow session with load_id: %s", load_id)
    # Placeholder for actual registration logic
    # e.g., insert into metadata table


@task(retries=3, retry_delay_seconds=30)
def wait_for_success_end(poll_interval: int = 60, timeout: int = 86400) -> bool:
    """
    Simulate an ExternalTaskSensor that waits for the previous day's DAG run to complete.
    """
    logger.info("Waiting for previous day DAG execution to succeed...")
    elapsed = 0
    while elapsed < timeout:
        # Placeholder for actual external check
        previous_success = True  # Replace with real check
        if previous_success:
            logger.info("Previous day DAG execution succeeded.")
            return True
        time.sleep(poll_interval)
        elapsed += poll_interval
    raise TimeoutError("Timed out waiting for previous day DAG execution.")


@task
def run_sys_kill_all_session_pg() -> None:
    """
    Trigger a system utility DAG to kill all PostgreSQL sessions.
    """
    logger.info("Triggering sys_kill_all_session_pg DAG...")
    # Placeholder for actual trigger logic
    # e.g., HTTP request to Airflow API


@task
def run_wf_data_preparation_for_reports() -> None:
    """
    Launch the data preparation workflow for reports.
    """
    logger.info("Triggering wf_data_preparation_for_reports DAG...")
    # Placeholder for actual trigger logic


@task
def load_ds_client_segmentation() -> int:
    """
    Trigger the client segmentation data loading workflow and return row count.
    """
    logger.info("Triggering client segmentation DAG...")
    # Placeholder for actual trigger logic
    # Simulate processing time
    time.sleep(5)
    row_count = 12345  # Replace with actual row count from the process
    logger.info("Client segmentation completed with %d rows.", row_count)
    return row_count


@task
def send_flg_to_sap(row_count: int) -> None:
    """
    Send a completion flag to the SAP system with the row count.
    """
    logger.info("Sending completion flag to SAP with row count: %d", row_count)
    sap_endpoint = os.getenv("SAP_ENDPOINT", "https://sap.example.com/notify")
    payload = {"status": "completed", "row_count": row_count}
    try:
        response = requests.post(sap_endpoint, json=payload, timeout=30)
        response.raise_for_status()
        logger.info("SAP notification succeeded with response code %d.", response.status_code)
    except Exception as exc:
        logger.error("Failed to notify SAP: %s", exc)
        raise


@task
def end_workflow(load_id: str) -> None:
    """
    Update metadata tables to mark the workflow session as successfully completed.
    """
    logger.info("Marking workflow session %s as completed.", load_id)
    # Placeholder for actual update logic
    # e.g., update status column in metadata table


@task
def email_on_failure(error_msg: str) -> None:
    """
    Send a failure notification email.
    """
    logger.info("Sending failure notification email.")
    smtp_server = os.getenv("SMTP_SERVER", "smtp.example.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USER", "alert@example.com")
    smtp_password = os.getenv("SMTP_PASSWORD", "password")
    recipients = os.getenv("FAILURE_EMAIL_RECIPIENTS", "admin@example.com").split(",")

    msg = EmailMessage()
    msg["Subject"] = "Workflow Failure Notification"
    msg["From"] = smtp_user
    msg["To"] = ", ".join(recipients)
    msg.set_content(f"The workflow failed with the following error:\n\n{error_msg}")

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.send_message(msg)
        logger.info("Failure email sent successfully.")
    except Exception as exc:
        logger.error("Failed to send failure email: %s", exc)


@flow
def main_workflow() -> None:
    """
    Main Prefect flow orchestrating the data loading, segmentation, and SAP integration pipeline.
    """
    try:
        wait_for_l2_full_load()
        load_id = get_load_id()
        workflow_registration(load_id)
        wait_for_success_end()
        run_sys_kill_all_session_pg()

        # Parallel branches
        data_prep_future = run_wf_data_preparation_for_reports.submit()
        segmentation_future = load_ds_client_segmentation.submit()

        # Wait for segmentation to finish and then send flag to SAP
        row_count = segmentation_future.result()
        send_flg_to_sap(row_count)

        # Ensure data preparation branch completes
        data_prep_future.result()

        end_workflow(load_id)

    except Exception as exc:
        error_message = str(exc)
        logger.exception("Workflow encountered an error: %s", error_message)
        email_on_failure(error_message)
        raise


if __name__ == "__main__":
    # Manual trigger; no schedule configured.
    main_workflow()