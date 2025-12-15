import logging
from datetime import timedelta
from typing import List

import requests
from prefect import flow, task
from prefect.triggers import always_run

logging.basicConfig(level=logging.INFO)


@task
def check_pcd_sftp_folder() -> bool:
    """Verify the presence of files in the designated SFTP folder."""
    logging.info("Checking SFTP folder for files...")
    # Placeholder implementation; replace with actual Kubernetes job logic.
    return True


@task
def check_pcd_shared_folder() -> bool:
    """Verify the presence of files in the shared directory."""
    logging.info("Checking shared folder for files...")
    # Placeholder implementation; replace with actual Kubernetes job logic.
    return True


@task
def start_pcd_extract_1() -> None:
    """Synchronization point marking the start of parallel extraction."""
    logging.info("Starting parallel API extraction phase (Extract 1).")
    return None


def _fetch_api(endpoint: str) -> dict:
    """Utility to fetch data from an HTTP API endpoint."""
    try:
        response = requests.get(endpoint, timeout=30)
        response.raise_for_status()
        logging.info("Fetched data from %s", endpoint)
        return response.json()
    except Exception as exc:
        logging.error("Error fetching %s: %s", endpoint, exc)
        raise


@task
def status_tracker() -> dict:
    return _fetch_api("https://api.example.com/status_tracker")


@task
def financial_expense() -> dict:
    return _fetch_api("https://api.example.com/financial_expense")


@task
def upcc_financial_reportingr() -> dict:
    return _fetch_api("https://api.example.com/upcc_financial_reportingr")


@task
def chc_financial_reporting() -> dict:
    return _fetch_api("https://api.example.com/chc_financial_reporting")


@task
def pcn_financial_reporting() -> dict:
    return _fetch_api("https://api.example.com/pcn_financial_reporting")


@task
def nppcc_financial_reporting() -> dict:
    return _fetch_api("https://api.example.com/nppcc_financial_reporting")


@task
def fiscal_year_reporting_dates() -> dict:
    return _fetch_api("https://api.example.com/fiscal_year_reporting_dates")


@task
def upcc_primary_care_patient_services() -> dict:
    return _fetch_api("https://api.example.com/upcc_primary_care_patient_services")


@task
def chc_primary_care_patient_services() -> dict:
    return _fetch_api("https://api.example.com/chc_primary_care_patient_services")


@task
def practitioner_role_mapping() -> dict:
    return _fetch_api("https://api.example.com/practitioner_role_mapping")


@task
def hr_records() -> dict:
    return _fetch_api("https://api.example.com/hr_records")


@task
def provincial_risk_tracking() -> dict:
    return _fetch_api("https://api.example.com/provincial_risk_tracking")


@task
def decision_log() -> dict:
    return _fetch_api("https://api.example.com/decision_log")


@task
def ha_hierarchy() -> dict:
    return _fetch_api("https://api.example.com/ha_hierarchy")


@task
def uppc_budget() -> dict:
    return _fetch_api("https://api.example.com/uppc_budget")


@task
def chc_budget() -> dict:
    return _fetch_api("https://api.example.com/chc_budget")


@task
def pcn_budget() -> dict:
    return _fetch_api("https://api.example.com/pcn_budget")


@task
def nppcc_budget() -> dict:
    return _fetch_api("https://api.example.com/nppcc_budget")


@task
def start_pcd_extract_2() -> None:
    """Synchronization point triggered after Status_Tracker completes."""
    logging.info("Starting second extraction synchronization point (Extract 2).")
    return None


@task
def pcd_file_upload(
    extraction_results: List[dict],
) -> bool:
    """Perform the main ETL file upload operation."""
    logging.info("Uploading consolidated ETL file with %d extraction results.", len(extraction_results))
    # Placeholder for actual Kubernetes job execution.
    return True


@task(trigger=always_run)
def etl_notification(upload_success: bool) -> None:
    """Send an email notification about the ETL run outcome."""
    if upload_success:
        logging.info("ETL succeeded – sending success email.")
    else:
        logging.warning("ETL failed – sending failure email.")
    # Placeholder: integrate with actual email backend here.


@flow
def pcd_etl_flow() -> None:
    """Orchestrates the PCD ETL pipeline."""
    # Step 1: Folder checks
    sftp_ok = check_pcd_sftp_folder()
    shared_ok = check_pcd_shared_folder(wait_for=[sftp_ok])

    # Step 2: Start parallel extraction
    start1 = start_pcd_extract_1(wait_for=[shared_ok])

    # Parallel API extraction tasks
    extraction_futures = [
        status_tracker.submit(wait_for=[start1]),
        financial_expense.submit(wait_for=[start1]),
        upcc_financial_reportingr.submit(wait_for=[start1]),
        chc_financial_reporting.submit(wait_for=[start1]),
        pcn_financial_reporting.submit(wait_for=[start1]),
        nppcc_financial_reporting.submit(wait_for=[start1]),
        fiscal_year_reporting_dates.submit(wait_for=[start1]),
        upcc_primary_care_patient_services.submit(wait_for=[start1]),
        chc_primary_care_patient_services.submit(wait_for=[start1]),
        practitioner_role_mapping.submit(wait_for=[start1]),
        hr_records.submit(wait_for=[start1]),
        provincial_risk_tracking.submit(wait_for=[start1]),
        decision_log.submit(wait_for=[start1]),
        ha_hierarchy.submit(wait_for=[start1]),
        uppc_budget.submit(wait_for=[start1]),
        chc_budget.submit(wait_for=[start1]),
        pcn_budget.submit(wait_for=[start1]),
        nppcc_budget.submit(wait_for=[start1]),
    ]

    # Step 3: Second synchronization point after Status_Tracker
    start2 = start_pcd_extract_2.submit(wait_for=[extraction_futures[0]])

    # Step 4: File upload depends on all extraction tasks and start2
    upload = pcd_file_upload.submit(
        extraction_results=[f.result() for f in extraction_futures],
        wait_for=extraction_futures + [start2],
    )

    # Step 5: Notification runs regardless of upstream success/failure
    etl_notification.submit(upload_success=upload, wait_for=[upload] + extraction_futures)


# Note: Deployment schedule can be configured via Prefect deployment UI or CLI,
# using the `pcd_etl_schedule` variable from the environment if needed.

if __name__ == "__main__":
    pcd_etl_flow()