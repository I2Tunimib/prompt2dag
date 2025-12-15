from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_email import EmailSend

# Task definitions
@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def check_pcd_sftp_folder():
    """Verifies the presence of files in a designated SFTP folder using a Kubernetes job."""
    # Placeholder for Kubernetes job operator
    return "SFTP folder check completed"

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def check_pcd_shared_folder(sftp_check_result):
    """Checks for files in a shared directory, dependent on the SFTP check."""
    # Placeholder for Kubernetes job operator
    return "Shared folder check completed"

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def start_pcd_extract_1():
    """Synchronization point for the beginning of the parallel API extraction phase."""
    return "Parallel API extraction started"

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def status_tracker():
    """Tracks the status of the ETL process."""
    return "Status tracker completed"

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def financial_expense():
    """Extracts financial expense data from an HTTP API endpoint."""
    return "Financial expense data extracted"

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def upcc_financial_reporting():
    """Extracts UPCC financial reporting data from an HTTP API endpoint."""
    return "UPCC financial reporting data extracted"

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def chc_financial_reporting():
    """Extracts CHC financial reporting data from an HTTP API endpoint."""
    return "CHC financial reporting data extracted"

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def pcn_financial_reporting():
    """Extracts PCN financial reporting data from an HTTP API endpoint."""
    return "PCN financial reporting data extracted"

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def nppcc_financial_reporting():
    """Extracts NPPCC financial reporting data from an HTTP API endpoint."""
    return "NPPCC financial reporting data extracted"

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def fiscal_year_reporting_dates():
    """Extracts fiscal year reporting dates from an HTTP API endpoint."""
    return "Fiscal year reporting dates extracted"

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def upcc_primary_care_patient_services():
    """Extracts UPCC primary care patient services data from an HTTP API endpoint."""
    return "UPCC primary care patient services data extracted"

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def chc_primary_care_patient_services():
    """Extracts CHC primary care patient services data from an HTTP API endpoint."""
    return "CHC primary care patient services data extracted"

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def practitioner_role_mapping():
    """Extracts practitioner role mapping data from an HTTP API endpoint."""
    return "Practitioner role mapping data extracted"

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def hr_records():
    """Extracts HR records data from an HTTP API endpoint."""
    return "HR records data extracted"

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def provincial_risk_tracking():
    """Extracts provincial risk tracking data from an HTTP API endpoint."""
    return "Provincial risk tracking data extracted"

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def decision_log():
    """Extracts decision log data from an HTTP API endpoint."""
    return "Decision log data extracted"

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def ha_hierarchy():
    """Extracts HA hierarchy data from an HTTP API endpoint."""
    return "HA hierarchy data extracted"

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def uppc_budget():
    """Extracts UPPC budget data from an HTTP API endpoint."""
    return "UPPC budget data extracted"

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def chc_budget():
    """Extracts CHC budget data from an HTTP API endpoint."""
    return "CHC budget data extracted"

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def pcn_budget():
    """Extracts PCN budget data from an HTTP API endpoint."""
    return "PCN budget data extracted"

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def nppcc_budget():
    """Extracts NPPCC budget data from an HTTP API endpoint."""
    return "NPPCC budget data extracted"

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def start_pcd_extract_2(status_tracker_result):
    """Synchronization point after the status tracker task."""
    return "Second parallel API extraction started"

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def pcd_file_upload(extraction_results):
    """Performs the main ETL file upload operation using a Kubernetes job."""
    # Placeholder for Kubernetes job operator
    return "ETL file upload completed"

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def etl_notification(extraction_results, upload_result):
    """Sends an email notification with the final status of the ETL pipeline."""
    # Placeholder for email notification
    subject = "ETL Pipeline Notification"
    message = "The ETL pipeline has completed successfully."
    if any(result == "failed" for result in extraction_results + [upload_result]):
        message = "The ETL pipeline has completed with failures."
    EmailSend(subject=subject, message=message, to=["example@example.com"])
    return "Notification sent"

# Flow definition
@flow(timeout_seconds=3600)
def pcd_etl_pipeline():
    """ETL pipeline for PCD (Primary Care Data) that orchestrates data extraction and file processing."""
    sftp_check_result = check_pcd_sftp_folder()
    shared_folder_check_result = check_pcd_shared_folder(sftp_check_result)
    
    start_extract_1 = start_pcd_extract_1()
    
    status_tracker_result = status_tracker.submit()
    financial_expense_result = financial_expense.submit()
    upcc_financial_reporting_result = upcc_financial_reporting.submit()
    chc_financial_reporting_result = chc_financial_reporting.submit()
    pcn_financial_reporting_result = pcn_financial_reporting.submit()
    nppcc_financial_reporting_result = nppcc_financial_reporting.submit()
    fiscal_year_reporting_dates_result = fiscal_year_reporting_dates.submit()
    upcc_primary_care_patient_services_result = upcc_primary_care_patient_services.submit()
    chc_primary_care_patient_services_result = chc_primary_care_patient_services.submit()
    practitioner_role_mapping_result = practitioner_role_mapping.submit()
    hr_records_result = hr_records.submit()
    provincial_risk_tracking_result = provincial_risk_tracking.submit()
    decision_log_result = decision_log.submit()
    ha_hierarchy_result = ha_hierarchy.submit()
    uppc_budget_result = uppc_budget.submit()
    chc_budget_result = chc_budget.submit()
    pcn_budget_result = pcn_budget.submit()
    nppcc_budget_result = nppcc_budget.submit()
    
    start_extract_2 = start_pcd_extract_2(status_tracker_result)
    
    extraction_results = [
        status_tracker_result,
        financial_expense_result,
        upcc_financial_reporting_result,
        chc_financial_reporting_result,
        pcn_financial_reporting_result,
        nppcc_financial_reporting_result,
        fiscal_year_reporting_dates_result,
        upcc_primary_care_patient_services_result,
        chc_primary_care_patient_services_result,
        practitioner_role_mapping_result,
        hr_records_result,
        provincial_risk_tracking_result,
        decision_log_result,
        ha_hierarchy_result,
        uppc_budget_result,
        chc_budget_result,
        pcn_budget_result,
        nppcc_budget_result
    ]
    
    upload_result = pcd_file_upload(extraction_results)
    
    etl_notification(extraction_results, upload_result)

if __name__ == '__main__':
    pcd_etl_pipeline()