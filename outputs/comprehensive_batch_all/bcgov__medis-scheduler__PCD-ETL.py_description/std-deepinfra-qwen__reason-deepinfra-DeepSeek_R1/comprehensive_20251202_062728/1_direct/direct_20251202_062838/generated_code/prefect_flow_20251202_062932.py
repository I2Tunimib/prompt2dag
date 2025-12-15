from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_kubernetes import KubernetesJob
from prefect_email import EmailSend

# Task definitions
@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=60))
def check_pcd_sftp_folder():
    """Verifies the presence of files in a designated SFTP folder using a Kubernetes job."""
    k8s_job = KubernetesJob(
        job="check-pcd-sftp-folder-job",
        namespace="default",
        manifest_path="path/to/sftp-check-job.yaml"
    )
    k8s_job.run()

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=60))
def check_pcd_shared_folder():
    """Checks for files in a shared directory, dependent on the SFTP check."""
    k8s_job = KubernetesJob(
        job="check-pcd-shared-folder-job",
        namespace="default",
        manifest_path="path/to/shared-folder-check-job.yaml"
    )
    k8s_job.run()

@task
def start_pcd_extract_1():
    """Synchronization point for the beginning of the parallel API extraction phase."""
    pass

@task
def status_tracker():
    """Tracks the status of the ETL process."""
    pass

@task
def financial_expense():
    """Extracts financial expense data from an HTTP API."""
    pass

@task
def upcc_financial_reporting():
    """Extracts UPCC financial reporting data from an HTTP API."""
    pass

@task
def chc_financial_reporting():
    """Extracts CHC financial reporting data from an HTTP API."""
    pass

@task
def pcn_financial_reporting():
    """Extracts PCN financial reporting data from an HTTP API."""
    pass

@task
def nppcc_financial_reporting():
    """Extracts NPPCC financial reporting data from an HTTP API."""
    pass

@task
def fiscal_year_reporting_dates():
    """Extracts fiscal year reporting dates from an HTTP API."""
    pass

@task
def upcc_primary_care_patient_services():
    """Extracts UPCC primary care patient services data from an HTTP API."""
    pass

@task
def chc_primary_care_patient_services():
    """Extracts CHC primary care patient services data from an HTTP API."""
    pass

@task
def practitioner_role_mapping():
    """Extracts practitioner role mapping data from an HTTP API."""
    pass

@task
def hr_records():
    """Extracts HR records from an HTTP API."""
    pass

@task
def provincial_risk_tracking():
    """Extracts provincial risk tracking data from an HTTP API."""
    pass

@task
def decision_log():
    """Extracts decision log data from an HTTP API."""
    pass

@task
def ha_hierarchy():
    """Extracts HA hierarchy data from an HTTP API."""
    pass

@task
def uppc_budget():
    """Extracts UPPC budget data from an HTTP API."""
    pass

@task
def chc_budget():
    """Extracts CHC budget data from an HTTP API."""
    pass

@task
def pcn_budget():
    """Extracts PCN budget data from an HTTP API."""
    pass

@task
def nppcc_budget():
    """Extracts NPPCC budget data from an HTTP API."""
    pass

@task
def start_pcd_extract_2():
    """Additional synchronization point after the status tracker task."""
    pass

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(minutes=60))
def pcd_file_upload():
    """Performs the main ETL file upload operation using a Kubernetes job."""
    k8s_job = KubernetesJob(
        job="pcd-file-upload-job",
        namespace="default",
        manifest_path="path/to/file-upload-job.yaml"
    )
    k8s_job.run()

@task
def etl_notification(success: bool):
    """Sends an email notification with the final status of the ETL process."""
    email = EmailSend(
        subject="ETL Pipeline Notification",
        email_to="recipient@example.com",
        body=f"ETL Pipeline {'succeeded' if success else 'failed'}."
    )
    email.run()

# Flow definition
@flow(timeout_seconds=3600)
def pcd_etl_pipeline():
    logger = get_run_logger()
    logger.info("Starting PCD ETL Pipeline")

    # Step 1: Check SFTP and Shared Folders
    sftp_check = check_pcd_sftp_folder.submit()
    shared_folder_check = check_pcd_shared_folder.submit(sftp_check)

    # Step 2: Start Parallel API Extraction
    start_extract_1 = start_pcd_extract_1.submit()

    # Step 3: Parallel HTTP API Extraction Tasks
    status_tracker_task = status_tracker.submit(start_extract_1)
    financial_expense_task = financial_expense.submit(start_extract_1)
    upcc_financial_reporting_task = upcc_financial_reporting.submit(start_extract_1)
    chc_financial_reporting_task = chc_financial_reporting.submit(start_extract_1)
    pcn_financial_reporting_task = pcn_financial_reporting.submit(start_extract_1)
    nppcc_financial_reporting_task = nppcc_financial_reporting.submit(start_extract_1)
    fiscal_year_reporting_dates_task = fiscal_year_reporting_dates.submit(start_extract_1)
    upcc_primary_care_patient_services_task = upcc_primary_care_patient_services.submit(start_extract_1)
    chc_primary_care_patient_services_task = chc_primary_care_patient_services.submit(start_extract_1)
    practitioner_role_mapping_task = practitioner_role_mapping.submit(start_extract_1)
    hr_records_task = hr_records.submit(start_extract_1)
    provincial_risk_tracking_task = provincial_risk_tracking.submit(start_extract_1)
    decision_log_task = decision_log.submit(start_extract_1)
    ha_hierarchy_task = ha_hierarchy.submit(start_extract_1)
    uppc_budget_task = uppc_budget.submit(start_extract_1)
    chc_budget_task = chc_budget.submit(start_extract_1)
    pcn_budget_task = pcn_budget.submit(start_extract_1)
    nppcc_budget_task = nppcc_budget.submit(start_extract_1)

    # Step 4: Additional Synchronization Point
    start_extract_2 = start_pcd_extract_2.submit(status_tracker_task)

    # Step 5: PCD File Upload
    file_upload_task = pcd_file_upload.submit(
        start_extract_2,
        financial_expense_task,
        upcc_financial_reporting_task,
        chc_financial_reporting_task,
        pcn_financial_reporting_task,
        nppcc_financial_reporting_task,
        fiscal_year_reporting_dates_task,
        upcc_primary_care_patient_services_task,
        chc_primary_care_patient_services_task,
        practitioner_role_mapping_task,
        hr_records_task,
        provincial_risk_tracking_task,
        decision_log_task,
        ha_hierarchy_task,
        uppc_budget_task,
        chc_budget_task,
        pcn_budget_task,
        nppcc_budget_task
    )

    # Step 6: ETL Notification
    etl_notification.submit(file_upload_task)

if __name__ == '__main__':
    pcd_etl_pipeline()