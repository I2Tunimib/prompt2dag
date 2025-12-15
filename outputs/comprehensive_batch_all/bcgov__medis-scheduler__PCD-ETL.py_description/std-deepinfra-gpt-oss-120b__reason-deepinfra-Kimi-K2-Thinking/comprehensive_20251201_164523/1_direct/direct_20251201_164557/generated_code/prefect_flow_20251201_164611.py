import logging
from typing import List

from prefect import flow, task, get_run_logger

logger = logging.getLogger(__name__)


@task
def check_pcd_sftp_folder() -> bool:
    """Simulate a Kubernetes job that checks the SFTP folder."""
    log = get_run_logger()
    log.info("Checking PCD SFTP folder...")
    # Placeholder for actual Kubernetes job execution
    return True


@task
def check_pcd_shared_folder() -> bool:
    """Simulate a Kubernetes job that checks the shared folder."""
    log = get_run_logger()
    log.info("Checking PCD shared folder...")
    # Placeholder for actual Kubernetes job execution
    return True


@task
def start_pcd_extract_1() -> None:
    """Synchronization point before parallel extraction."""
    log = get_run_logger()
    log.info("Starting parallel PCD extraction (phase 1).")


@task
def status_tracker() -> bool:
    """Extract status tracker data via HTTP API."""
    log = get_run_logger()
    log.info("Running Status_Tracker extraction...")
    return True


@task
def financial_expense() -> bool:
    log = get_run_logger()
    log.info("Running Financial_Expense extraction...")
    return True


@task
def upcc_financial_reportingr() -> bool:
    log = get_run_logger()
    log.info("Running UPCC_Financial_Reportingr extraction...")
    return True


@task
def chc_financial_reporting() -> bool:
    log = get_run_logger()
    log.info("Running CHC_Financial_reporting extraction...")
    return True


@task
def pcn_financial_reporting() -> bool:
    log = get_run_logger()
    log.info("Running PCN_Financial_Reporting extraction...")
    return True


@task
def nppcc_financial_reporting() -> bool:
    log = get_run_logger()
    log.info("Running NPPCC_Financial_Reporting extraction...")
    return True


@task
def fiscal_year_reporting_dates() -> bool:
    log = get_run_logger()
    log.info("Running Fiscal_Year_Reporting_Dates extraction...")
    return True


@task
def upcc_primary_care_patient_services() -> bool:
    log = get_run_logger()
    log.info("Running UPCC_Primary_Care_Patient_Services extraction...")
    return True


@task
def chc_primary_care_patient_services() -> bool:
    log = get_run_logger()
    log.info("Running CHC_Primary_Care_Patient_Services extraction...")
    return True


@task
def practitioner_role_mapping() -> bool:
    log = get_run_logger()
    log.info("Running Practitioner_Role_Mapping extraction...")
    return True


@task
def hr_records() -> bool:
    log = get_run_logger()
    log.info("Running HR_Records extraction...")
    return True


@task
def provincial_risk_tracking() -> bool:
    log = get_run_logger()
    log.info("Running Provincial_Risk_Tracking extraction...")
    return True


@task
def decision_log() -> bool:
    log = get_run_logger()
    log.info("Running Decision_Log extraction...")
    return True


@task
def ha_hierarchy() -> bool:
    log = get_run_logger()
    log.info("Running HA_Hierarchy extraction...")
    return True


@task
def uppc_budget() -> bool:
    log = get_run_logger()
    log.info("Running UPPC_Budget extraction...")
    return True


@task
def chc_budget() -> bool:
    log = get_run_logger()
    log.info("Running CHC_Budget extraction...")
    return True


@task
def pcn_budget() -> bool:
    log = get_run_logger()
    log.info("Running PCN_Budget extraction...")
    return True


@task
def nppcc_budget() -> bool:
    log = get_run_logger()
    log.info("Running NPPCC_Budget extraction...")
    return True


@task
def start_pcd_extract_2() -> None:
    """Synchronization point after Status_Tracker."""
    log = get_run_logger()
    log.info("Starting second extraction phase after Status_Tracker.")


@task
def pcd_file_upload() -> bool:
    """Simulate a Kubernetes job that uploads the consolidated file."""
    log = get_run_logger()
    log.info("Executing PCD file upload...")
    # Placeholder for actual upload logic
    return True


@task
def etl_notification(success: bool, failed_tasks: List[str]) -> None:
    """Send an email notification about the ETL run outcome."""
    log = get_run_logger()
    if success:
        log.info("ETL completed successfully. Sending success email.")
    else:
        log.error(
            "ETL completed with failures in tasks: %s. Sending failure email.",
            ", ".join(failed_tasks),
        )
    # Placeholder for email sending logic


@flow
def pcd_etl_flow() -> None:
    """
    Prefect flow orchestrating the PCD ETL pipeline.
    """
    log = get_run_logger()
    overall_success = True
    failed_tasks: List[str] = []

    # Sequential checks
    check_pcd_sftp_folder()
    check_pcd_shared_folder()

    # Start parallel extraction phase
    start_pcd_extract_1()

    # Launch parallel extraction tasks
    extraction_futures = {
        "status_tracker": status_tracker.submit(),
        "financial_expense": financial_expense.submit(),
        "upcc_financial_reportingr": upcc_financial_reportingr.submit(),
        "chc_financial_reporting": chc_financial_reporting.submit(),
        "pcn_financial_reporting": pcn_financial_reporting.submit(),
        "nppcc_financial_reporting": nppcc_financial_reporting.submit(),
        "fiscal_year_reporting_dates": fiscal_year_reporting_dates.submit(),
        "upcc_primary_care_patient_services": upcc_primary_care_patient_services.submit(),
        "chc_primary_care_patient_services": chc_primary_care_patient_services.submit(),
        "practitioner_role_mapping": practitioner_role_mapping.submit(),
        "hr_records": hr_records.submit(),
        "provincial_risk_tracking": provincial_risk_tracking.submit(),
        "decision_log": decision_log.submit(),
        "ha_hierarchy": ha_hierarchy.submit(),
        "uppc_budget": uppc_budget.submit(),
        "chc_budget": chc_budget.submit(),
        "pcn_budget": pcn_budget.submit(),
        "nppcc_budget": nppcc_budget.submit(),
    }

    # Wait for Status_Tracker to finish before starting the second sync point
    try:
        extraction_futures["status_tracker"].result()
    except Exception as exc:
        overall_success = False
        failed_tasks.append("status_tracker")
        log.error("Status_Tracker failed: %s", exc)

    start_pcd_extract_2()

    # Wait for all other extraction tasks
    for name, future in extraction_futures.items():
        if name == "status_tracker":
            continue  # already handled
        try:
            future.result()
        except Exception as exc:
            overall_success = False
            failed_tasks.append(name)
            log.error("%s failed: %s", name, exc)

    # Run the file upload if all extraction tasks succeeded
    if overall_success:
        try:
            pcd_file_upload()
        except Exception as exc:
            overall_success = False
            failed_tasks.append("pcd_file_upload")
            log.error("PCD file upload failed: %s", exc)
    else:
        log.warning("Skipping file upload due to previous failures.")

    # Notification runs regardless of success/failure
    etl_notification(overall_success, failed_tasks)


# Note: Deployment schedule can be configured via Prefect deployment UI or CLI,
# using the `pcd_etl_schedule` variable to set a cron expression if desired.

if __name__ == "__main__":
    pcd_etl_flow()