from __future__ import annotations

import os
import smtplib
from email.message import EmailMessage
from typing import List

import requests
from prefect import flow, task, get_run_logger
from prefect.futures import PrefectFuture


@task
def check_pcd_sftp_folder() -> bool:
    """Simulate checking the SFTP folder for required files."""
    logger = get_run_logger()
    logger.info("Checking PCD SFTP folder for files...")
    # Placeholder logic – replace with actual SFTP check if needed
    return True


@task
def check_pcd_shared_folder() -> bool:
    """Simulate checking the shared folder for required files."""
    logger = get_run_logger()
    logger.info("Checking PCD shared folder for files...")
    # Placeholder logic – replace with actual shared folder check if needed
    return True


@task
def start_pcd_extract_1() -> bool:
    """Synchronization point marking the start of parallel extraction."""
    logger = get_run_logger()
    logger.info("Starting parallel PCD extraction phase (Extract 1).")
    return True


def _http_get(endpoint_name: str) -> bool:
    """Helper to perform a GET request to a configured endpoint."""
    logger = get_run_logger()
    url = os.getenv(f"{endpoint_name.upper()}_URL")
    if not url:
        logger.warning("No URL configured for %s; skipping request.", endpoint_name)
        return False
    try:
        logger.info("Fetching data from %s (%s)", endpoint_name, url)
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        logger.info("Successfully fetched data from %s", endpoint_name)
        return True
    except Exception as exc:  # pragma: no cover
        logger.error("Error fetching %s: %s", endpoint_name, exc)
        raise


@task
def status_tracker() -> bool:
    return _http_get("status_tracker")


@task
def financial_expense() -> bool:
    return _http_get("financial_expense")


@task
def upcc_financial_reportingr() -> bool:
    return _http_get("upcc_financial_reportingr")


@task
def chc_financial_reporting() -> bool:
    return _http_get("chc_financial_reporting")


@task
def pcn_financial_reporting() -> bool:
    return _http_get("pcn_financial_reporting")


@task
def nppcc_financial_reporting() -> bool:
    return _http_get("nppcc_financial_reporting")


@task
def fiscal_year_reporting_dates() -> bool:
    return _http_get("fiscal_year_reporting_dates")


@task
def upcc_primary_care_patient_services() -> bool:
    return _http_get("upcc_primary_care_patient_services")


@task
def chc_primary_care_patient_services() -> bool:
    return _http_get("chc_primary_care_patient_services")


@task
def practitioner_role_mapping() -> bool:
    return _http_get("practitioner_role_mapping")


@task
def hr_records() -> bool:
    return _http_get("hr_records")


@task
def provincial_risk_tracking() -> bool:
    return _http_get("provincial_risk_tracking")


@task
def decision_log() -> bool:
    return _http_get("decision_log")


@task
def ha_hierarchy() -> bool:
    return _http_get("ha_hierarchy")


@task
def uppc_budget() -> bool:
    return _http_get("uppc_budget")


@task
def chc_budget() -> bool:
    return _http_get("chc_budget")


@task
def pcn_budget() -> bool:
    return _http_get("pcn_budget")


@task
def nppcc_budget() -> bool:
    return _http_get("nppcc_budget")


@task
def start_pcd_extract_2(status_tracker_result: bool) -> bool:
    """Synchronization point triggered after Status_Tracker completes."""
    logger = get_run_logger()
    logger.info("Starting second synchronization point (Extract 2).")
    return True


@task
def pcd_file_upload() -> bool:
    """Simulate the Kubernetes job that uploads the consolidated ETL file."""
    logger = get_run_logger()
    logger.info("Submitting Kubernetes job for PCD file upload...")
    # Placeholder – integrate with actual KubernetesJobOperator or client as needed
    return True


@task
def etl_notification(success: bool) -> None:
    """Send an email notification about the overall ETL run status."""
    logger = get_run_logger()
    subject = "PCD ETL Run Success" if success else "PCD ETL Run Failure"
    body = "The PCD ETL pipeline completed successfully." if success else (
        "The PCD ETL pipeline encountered errors. Please review the logs."
    )
    recipients = os.getenv("PCD_ETL_NOTIFICATION_EMAILS", "").split(",")
    if not recipients or recipients == [""]:
        logger.warning("No notification recipients configured; skipping email.")
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = os.getenv("PCD_ETL_EMAIL_SENDER", "no-reply@example.com")
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    try:
        smtp_host = os.getenv("SMTP_HOST", "localhost")
        smtp_port = int(os.getenv("SMTP_PORT", "25"))
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.send_message(msg)
        logger.info("Notification email sent to %s", recipients)
    except Exception as exc:  # pragma: no cover
        logger.error("Failed to send notification email: %s", exc)


@flow
def pcd_etl_flow() -> None:
    """Orchestrates the full PCD ETL pipeline."""
    logger = get_run_logger()
    overall_success = True

    try:
        # Sequential checks
        check_pcd_sftp_folder()
        check_pcd_shared_folder()
        start1 = start_pcd_extract_1()

        # Parallel HTTP API extraction tasks
        extraction_tasks: List[PrefectFuture] = [
            status_tracker.submit(),
            financial_expense.submit(),
            upcc_financial_reportingr.submit(),
            chc_financial_reporting.submit(),
            pcn_financial_reporting.submit(),
            nppcc_financial_reporting.submit(),
            fiscal_year_reporting_dates.submit(),
            upcc_primary_care_patient_services.submit(),
            chc_primary_care_patient_services.submit(),
            practitioner_role_mapping.submit(),
            hr_records.submit(),
            provincial_risk_tracking.submit(),
            decision_log.submit(),
            ha_hierarchy.submit(),
            uppc_budget.submit(),
            chc_budget.submit(),
            pcn_budget.submit(),
            nppcc_budget.submit(),
        ]

        # Ensure start2 runs after Status_Tracker completes
        status_tracker_future = extraction_tasks[0]
        start2 = start_pcd_extract_2.submit(wait_for=[status_tracker_future])

        # File upload after all extraction tasks and start2
        upload_future = pcd_file_upload.submit(
            wait_for=extraction_tasks + [start2]
        )

        # Await results to capture any failures
        for fut in extraction_tasks:
            fut.result()
        start2.result()
        upload_future.result()

    except Exception as exc:  # pragma: no cover
        logger.error("ETL flow encountered an error: %s", exc)
        overall_success = False

    finally:
        etl_notification(overall_success)


# Note: In a production deployment, configure a Prefect deployment with a schedule
# derived from the `pcd_etl_schedule` variable (cron expression or manual trigger).

if __name__ == "__main__":
    pcd_etl_flow()