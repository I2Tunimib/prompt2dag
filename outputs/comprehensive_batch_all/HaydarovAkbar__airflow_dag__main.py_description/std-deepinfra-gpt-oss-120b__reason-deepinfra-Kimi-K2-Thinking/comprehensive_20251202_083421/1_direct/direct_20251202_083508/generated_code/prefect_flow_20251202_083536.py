import logging
import os
import smtplib
import time
from email.message import EmailMessage

import requests
from prefect import flow, task

# Configure basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Helper functions (placeholders for real implementations)
# ----------------------------------------------------------------------


def check_l2_load_flag() -> bool:
    """
    Placeholder for checking the L2 load completion flag in the metadata table.
    Returns True if the load is complete.
    """
    # In a real implementation, query the metadata DB.
    return True


def check_previous_day_success() -> bool:
    """
    Placeholder for checking that the previous day's DAG run completed successfully.
    Returns True if the previous run succeeded.
    """
    # In a real implementation, query the orchestration metadata.
    return True


def trigger_dag(dag_name: str, **kwargs) -> None:
    """
    Placeholder for triggering another DAG.
    """
    logger.info("Triggering DAG '%s' with parameters %s", dag_name, kwargs)


def send_email(subject: str, body: str, recipients: list[str]) -> None:
    """
    Sends an email using SMTP. Adjust SMTP settings as needed.
    """
    smtp_host = os.getenv("SMTP_HOST", "localhost")
    smtp_port = int(os.getenv("SMTP_PORT", "25"))
    sender = os.getenv("SMTP_SENDER", "no-reply@example.com")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.send_message(msg)
    logger.info("Sent email to %s", recipients)


# ----------------------------------------------------------------------
# Prefect tasks
# ----------------------------------------------------------------------


@task
def wait_for_l2_full_load(poll_interval: int = 60, timeout: int = 3600) -> None:
    """
    Waits until the L1‑to‑L2 load flag is set.
    """
    logger.info("Waiting for L2 full load to complete...")
    elapsed = 0
    while not check_l2_load_flag():
        if elapsed >= timeout:
            raise TimeoutError("Timed out waiting for L2 full load.")
        time.sleep(poll_interval)
        elapsed += poll_interval
    logger.info("L2 full load completed.")


@task
def get_load_id() -> str:
    """
    Retrieves a unique load identifier for the current session.
    """
    load_id = f"load_{int(time.time())}"
    logger.info("Generated load ID: %s", load_id)
    return load_id


@task
def workflow_registration(load_id: str) -> None:
    """
    Registers the workflow session in metadata tables and initiates logging.
    """
    logger.info("Registering workflow session with load ID: %s", load_id)
    # Placeholder for actual registration logic (e.g., DB insert)


@task
def wait_for_success_end(poll_interval: int = 60, timeout: int = 86400) -> None:
    """
    Waits for the previous day's execution of this DAG to finish successfully.
    """
    logger.info("Waiting for previous day's DAG run to succeed...")
    elapsed = 0
    while not check_previous_day_success():
        if elapsed >= timeout:
            raise TimeoutError("Timed out waiting for previous day's DAG success.")
        time.sleep(poll_interval)
        elapsed += poll_interval
    logger.info("Previous day's DAG run succeeded.")


@task
def run_sys_kill_all_session_pg() -> None:
    """
    Triggers a system utility DAG that kills all PostgreSQL sessions.
    """
    trigger_dag("sys_kill_all_session_pg")
    logger.info("Triggered sys_kill_all_session_pg DAG.")


@task
def run_wf_data_preparation_for_reports() -> None:
    """
    Triggers the data preparation workflow for reports.
    """
    trigger_dag("wf_data_preparation_for_reports")
    logger.info("Triggered wf_data_preparation_for_reports DAG.")


@task
def load_ds_client_segmentation() -> dict:
    """
    Triggers the client segmentation data loading workflow and returns a result summary.
    """
    trigger_dag("l1_to_l2_p_load_data_ds_client_segmentation_full")
    logger.info("Triggered client segmentation DAG.")
    # Placeholder result; in reality, fetch row count or status from the DAG.
    result = {"row_count": 12345}
    return result


@task
def send_flg_to_sap(segmentation_result: dict) -> None:
    """
    Sends a completion flag to the SAP system, including the row count from segmentation.
    """
    sap_endpoint = os.getenv("SAP_ENDPOINT", "https://sap.example.com/notify")
    payload = {
        "status": "completed",
        "row_count": segmentation_result.get("row_count", 0),
    }
    logger.info("Sending completion flag to SAP: %s", payload)
    try:
        response = requests.post(sap_endpoint, json=payload, timeout=30)
        response.raise_for_status()
        logger.info("SAP notification succeeded with status %s.", response.status_code)
    except Exception as exc:
        logger.error("Failed to notify SAP: %s", exc)
        raise


@task
def end(load_id: str) -> None:
    """
    Updates metadata tables to mark the workflow session as successfully completed.
    """
    logger.info("Marking workflow session %s as completed.", load_id)
    # Placeholder for actual update logic (e.g., DB update)


@task
def email_on_failure(error: Exception) -> None:
    """
    Sends a failure notification email.
    """
    subject = "Workflow Failure Notification"
    body = f"The workflow failed with the following error:\n\n{error}"
    recipients = os.getenv("FAILURE_EMAIL_RECIPIENTS", "admin@example.com").split(",")
    send_email(subject, body, recipients)


# ----------------------------------------------------------------------
# Main flow
# ----------------------------------------------------------------------


@flow
def main_workflow() -> None:
    """
    Orchestrates the full pipeline from load monitoring to SAP notification.
    """
    try:
        wait_for_l2_full_load()
        load_id = get_load_id()
        workflow_registration(load_id)
        wait_for_success_end()
        run_sys_kill_all_session_pg()

        # Parallel branches
        data_prep_future = run_wf_data_preparation_for_reports.submit()
        seg_load_future = load_ds_client_segmentation.submit()

        # Continue segmentation branch after load completes
        seg_result = seg_load_future.result()
        send_flg_future = send_flg_to_sap.submit(seg_result)

        # Wait for both parallel branches to finish
        data_prep_future.result()
        send_flg_future.result()

        end(load_id)
    except Exception as exc:  # pragma: no cover
        email_on_failure(exc)
        raise


if __name__ == "__main__":
    main_workflow()