import requests
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from typing import List, Dict, Any


@task
def check_pcd_sftp_folder() -> bool:
    """Simulates checking SFTP folder via Kubernetes job."""
    # In production, this would trigger a Kubernetes job
    print("Checking PCD SFTP folder...")
    # Simulate successful check
    return True


@task
def check_pcd_shared_folder(sftp_check_result: bool) -> bool:
    """Simulates checking shared folder via Kubernetes job, depends on SFTP check."""
    print("Checking PCD shared folder...")
    if not sftp_check_result:
        raise ValueError("SFTP check failed, cannot proceed with shared folder check")
    return True


@task
def start_pcd_extract_1() -> None:
    """Synchronization point marking the beginning of parallel extraction phase."""
    print("Starting PCD Extract Phase 1...")


@task
def status_tracker() -> Dict[str, Any]:
    """Extracts data from Status Tracker API."""
    print("Extracting Status Tracker data...")
    # Simulate API call
    return {"status": "success", "records": 100}


@task
def financial_expense() -> Dict[str, Any]:
    """Extracts financial expense data."""
    print("Extracting Financial Expense data...")
    return {"status": "success", "records": 500}


@task
def upcc_financial_reporting() -> Dict[str, Any]:
    """Extracts UPCC financial reporting data."""
    print("Extracting UPCC Financial Reporting data...")
    return {"status": "success", "records": 300}


@task
def chc_financial_reporting() -> Dict[str, Any]:
    """Extracts CHC financial reporting data."""
    print("Extracting CHC Financial Reporting data...")
    return {"status": "success", "records": 250}


@task
def pcn_financial_reporting() -> Dict[str, Any]:
    """Extracts PCN financial reporting data."""
    print("Extracting PCN Financial Reporting data...")
    return {"status": "success", "records": 400}


@task
def nppcc_financial_reporting() -> Dict[str, Any]:
    """Extracts NPPCC financial reporting data."""
    print("Extracting NPPCC Financial Reporting data...")
    return {"status": "success", "records": 200}


@task
def fiscal_year_reporting_dates() -> Dict[str, Any]:
    """Extracts fiscal year reporting dates."""
    print("Extracting Fiscal Year Reporting Dates...")
    return {"status": "success", "records": 50}


@task
def upcc_primary_care_patient_services() -> Dict[str, Any]:
    """Extracts UPCC primary care patient services data."""
    print("Extracting UPCC Primary Care Patient Services data...")
    return {"status": "success", "records": 600}


@task
def chc_primary_care_patient_services() -> Dict[str, Any]:
    """Extracts CHC primary care patient services data."""
    print("Extracting CHC Primary Care Patient Services data...")
    return {"status": "success", "records": 550}


@task
def practitioner_role_mapping() -> Dict[str, Any]:
    """Extracts practitioner role mapping data."""
    print("Extracting Practitioner Role Mapping data...")
    return {"status": "success", "records": 150}


@task
def hr_records() -> Dict[str, Any]:
    """Extracts HR records data."""
    print("Extracting HR Records data...")
    return {"status": "success", "records": 800}


@task
def provincial_risk_tracking() -> Dict[str, Any]:
    """Extracts provincial risk tracking data."""
    print("Extracting Provincial Risk Tracking data...")
    return {"status": "success", "records": 120}


@task
def decision_log() -> Dict[str, Any]:
    """Extracts decision log data."""
    print("Extracting Decision Log data...")
    return {"status": "success", "records": 75}


@task
def ha_hierarchy() -> Dict[str, Any]:
    """Extracts HA hierarchy data."""
    print("Extracting HA Hierarchy data...")
    return {"status": "success", "records": 100}


@task
def uppc_budget() -> Dict[str, Any]:
    """Extracts UPPC budget data."""
    print("Extracting UPPC Budget data...")
    return {"status": "success", "records": 180}


@task
def chc_budget() -> Dict[str, Any]:
    """Extracts CHC budget data."""
    print("Extracting CHC Budget data...")
    return {"status": "success", "records": 160}


@task
def pcn_budget() -> Dict[str, Any]:
    """Extracts PCN budget data."""
    print("Extracting PCN Budget data...")
    return {"status": "success", "records": 200}


@task
def nppcc_budget() -> Dict[str, Any]:
    """Extracts NPPCC budget data."""
    print("Extracting NPPCC Budget data...")
    return {"status": "success", "records": 140}


@task
def start_pcd_extract_2(status_tracker_result: Dict[str, Any]) -> None:
    """Additional synchronization point triggered by Status_Tracker."""
    print("Starting PCD Extract Phase 2...")
    if status_tracker_result.get("status") != "success":
        print("Warning: Status Tracker indicated issues")


@task
def pcd_file_upload(extraction_results: List[Dict[str, Any]]) -> bool:
    """Simulates Kubernetes job for ETL file upload operation."""
    print("Performing PCD file upload...")
    # In production, this would trigger a Kubernetes job with all extraction results
    successful_extractions = sum(1 for r in extraction_results if r.get("status") == "success")
    print(f"Uploading data from {successful_extractions} successful extractions")
    return True


@task
def etl_notification(
    sftp_check: bool,
    shared_check: bool,
    extract_1: None,
    extract_2: None,
    upload_result: bool,
    extraction_results: List[Dict[str, Any]]
) -> None:
    """Sends email notification based on pipeline execution results."""
    print("Analyzing pipeline execution results...")
    
    # Determine overall status
    all_checks = sftp_check and shared_check
    all_extractions = all(r.get("status") == "success" for r in extraction_results)
    upload_success = upload_result
    
    if all_checks and all_extractions and upload_success:
        status = "SUCCESS"
        message = "PCD ETL pipeline completed successfully."
    else:
        status = "FAILURE"
        message = "PCD ETL pipeline encountered issues during execution."
    
    # Simulate email sending
    print(f"Sending {status} notification: {message}")
    print(f"Extraction summary: {len(extraction_results)} tasks processed")
    
    # In production, integrate with email system
    # e.g., send_email(to=email_list, subject=f"PCD ETL {status}", body=message)


@flow(
    name="pcd-etl-pipeline",
    description="ETL pipeline for Primary Care Data extraction and processing",
    timeout_seconds=3600,  # 60-minute timeout
    # Schedule can be configured via Prefect deployment
    # e.g., schedule={"cron": "0 2 * * *"} or schedule=None
)
def pcd_etl_pipeline():
    """Main PCD ETL pipeline flow."""
    
    # Step 1: Check SFTP folder
    sftp_check = check_pcd_sftp_folder.submit()
    
    # Step 2: Check shared folder (depends on SFTP check)
    shared_check = check_pcd_shared_folder.submit(sftp_check)
    
    # Step 3: Synchronization point for extraction phase
    extract_1 = start_pcd_extract_1.submit()
    
    # Step 4: Parallel HTTP API extraction tasks
    # All depend on extract_1 synchronization point
    extraction_tasks = []
    
    # Submit all parallel extraction tasks
    status_task = status_tracker.submit(wait_for=[extract_1])
    extraction_tasks.append(status_task)
    
    extraction_tasks.extend([
        financial_expense.submit(wait_for=[extract_1]),
        upcc_financial_reporting.submit(wait_for=[extract_1]),
        chc_financial_reporting.submit(wait_for=[extract_1]),
        pcn_financial_reporting.submit(wait_for=[extract_1]),
        nppcc_financial_reporting.submit(wait_for=[extract_1]),
        fiscal_year_reporting_dates.submit(wait_for=[extract_1]),
        upcc_primary_care_patient_services.submit(wait_for=[extract_1]),
        chc_primary_care_patient_services.submit(wait_for=[extract_1]),
        practitioner_role_mapping.submit(wait_for=[extract_1]),
        hr_records.submit(wait_for=[extract_1]),
        provincial_risk_tracking.submit(wait_for=[extract_1]),
        decision_log.submit(wait_for=[extract_1]),
        ha_hierarchy.submit(wait_for=[extract_1]),
        uppc_budget.submit(wait_for=[extract_1]),
        chc_budget.submit(wait_for=[extract_1]),
        pcn_budget.submit(wait_for=[extract_1]),
        nppcc_budget.submit(wait_for=[extract_1]),
    ])
    
    # Step 5: Additional synchronization point (depends on Status_Tracker)
    extract_2 = start_pcd_extract_2.submit(status_task, wait_for=[status_task])
    
    # Step 6: File upload (depends on all extraction tasks and extract_2)
    # Wait for all extraction tasks and extract_2 to complete
    upload_result = pcd_file_upload.submit(
        extraction_tasks,
        wait_for=[shared_check, extract_2] + extraction_tasks
    )
    
    # Step 7: Notification (depends on all upstream tasks, runs regardless of success/failure)
    # Use wait_for to ensure it runs after all upstream tasks
    etl_notification.submit(
        sftp_check,
        shared_check,
        extract_1,
        extract_2,
        upload_result,
        extraction_tasks,
        wait_for=[upload_result]
    )


if __name__ == "__main__":
    # For local execution and testing
    pcd_etl_pipeline()