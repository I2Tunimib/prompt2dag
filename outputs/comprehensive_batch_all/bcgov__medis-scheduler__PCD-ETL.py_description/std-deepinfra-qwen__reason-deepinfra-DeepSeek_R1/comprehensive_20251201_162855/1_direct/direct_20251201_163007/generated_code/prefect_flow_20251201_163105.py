from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_email import EmailNotification
from prefect_kubernetes import KubernetesJob

# Kubernetes Job Operators
@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def check_pcd_sftp_folder():
    k8s_job = KubernetesJob(
        job="check-pcd-sftp-folder",
        namespace="default",
        manifest_path="path/to/sftp-check-job.yaml"
    )
    k8s_job.run()

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def check_pcd_shared_folder():
    k8s_job = KubernetesJob(
        job="check-pcd-shared-folder",
        namespace="default",
        manifest_path="path/to/shared-folder-check-job.yaml"
    )
    k8s_job.run()

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def pcd_file_upload():
    k8s_job = KubernetesJob(
        job="pcd-file-upload",
        namespace="default",
        manifest_path="path/to/file-upload-job.yaml"
    )
    k8s_job.run()

# HTTP API Extraction Tasks
@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def status_tracker():
    # Placeholder for HTTP API call to Status Tracker
    pass

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def financial_expense():
    # Placeholder for HTTP API call to Financial Expense
    pass

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def upcc_financial_reporting():
    # Placeholder for HTTP API call to UPCC Financial Reporting
    pass

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def chc_financial_reporting():
    # Placeholder for HTTP API call to CHC Financial Reporting
    pass

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def pcn_financial_reporting():
    # Placeholder for HTTP API call to PCN Financial Reporting
    pass

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def nppcc_financial_reporting():
    # Placeholder for HTTP API call to NPPCC Financial Reporting
    pass

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def fiscal_year_reporting_dates():
    # Placeholder for HTTP API call to Fiscal Year Reporting Dates
    pass

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def upcc_primary_care_patient_services():
    # Placeholder for HTTP API call to UPCC Primary Care Patient Services
    pass

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def chc_primary_care_patient_services():
    # Placeholder for HTTP API call to CHC Primary Care Patient Services
    pass

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def practitioner_role_mapping():
    # Placeholder for HTTP API call to Practitioner Role Mapping
    pass

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def hr_records():
    # Placeholder for HTTP API call to HR Records
    pass

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def provincial_risk_tracking():
    # Placeholder for HTTP API call to Provincial Risk Tracking
    pass

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def decision_log():
    # Placeholder for HTTP API call to Decision Log
    pass

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def ha_hierarchy():
    # Placeholder for HTTP API call to HA Hierarchy
    pass

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def uppc_budget():
    # Placeholder for HTTP API call to UPPC Budget
    pass

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def chc_budget():
    # Placeholder for HTTP API call to CHC Budget
    pass

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def pcn_budget():
    # Placeholder for HTTP API call to PCN Budget
    pass

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def nppcc_budget():
    # Placeholder for HTTP API call to NPPCC Budget
    pass

# Notification Task
@task(trigger="all_done")
def etl_notification(success: bool):
    logger = get_run_logger()
    if success:
        logger.info("ETL Pipeline completed successfully.")
        EmailNotification(
            email="success@example.com",
            subject="ETL Pipeline Success",
            message="The ETL pipeline has completed successfully."
        ).run()
    else:
        logger.error("ETL Pipeline failed.")
        EmailNotification(
            email="failure@example.com",
            subject="ETL Pipeline Failure",
            message="The ETL pipeline has failed."
        ).run()

# Flow Definition
@flow(timeout_seconds=3600, validate_parameters=False)
def pcd_etl_pipeline():
    # Check PCD SFTP Folder
    sftp_check_result = check_pcd_sftp_folder.submit()

    # Check PCD Shared Folder
    shared_folder_check_result = check_pcd_shared_folder.submit(wait_for=[sftp_check_result])

    # Start PCD Extract 1
    start_pcd_extract_1 = status_tracker.submit()

    # Parallel HTTP API Extraction Tasks
    status_tracker_result = status_tracker.submit(wait_for=[start_pcd_extract_1])
    financial_expense_result = financial_expense.submit(wait_for=[start_pcd_extract_1])
    upcc_financial_reporting_result = upcc_financial_reporting.submit(wait_for=[start_pcd_extract_1])
    chc_financial_reporting_result = chc_financial_reporting.submit(wait_for=[start_pcd_extract_1])
    pcn_financial_reporting_result = pcn_financial_reporting.submit(wait_for=[start_pcd_extract_1])
    nppcc_financial_reporting_result = nppcc_financial_reporting.submit(wait_for=[start_pcd_extract_1])
    fiscal_year_reporting_dates_result = fiscal_year_reporting_dates.submit(wait_for=[start_pcd_extract_1])
    upcc_primary_care_patient_services_result = upcc_primary_care_patient_services.submit(wait_for=[start_pcd_extract_1])
    chc_primary_care_patient_services_result = chc_primary_care_patient_services.submit(wait_for=[start_pcd_extract_1])
    practitioner_role_mapping_result = practitioner_role_mapping.submit(wait_for=[start_pcd_extract_1])
    hr_records_result = hr_records.submit(wait_for=[start_pcd_extract_1])
    provincial_risk_tracking_result = provincial_risk_tracking.submit(wait_for=[start_pcd_extract_1])
    decision_log_result = decision_log.submit(wait_for=[start_pcd_extract_1])
    ha_hierarchy_result = ha_hierarchy.submit(wait_for=[start_pcd_extract_1])
    uppc_budget_result = uppc_budget.submit(wait_for=[start_pcd_extract_1])
    chc_budget_result = chc_budget.submit(wait_for=[start_pcd_extract_1])
    pcn_budget_result = pcn_budget.submit(wait_for=[start_pcd_extract_1])
    nppcc_budget_result = nppcc_budget.submit(wait_for=[start_pcd_extract_1])

    # Start PCD Extract 2
    start_pcd_extract_2 = status_tracker.submit(wait_for=[status_tracker_result])

    # PCD File Upload
    file_upload_result = pcd_file_upload.submit(
        wait_for=[
            start_pcd_extract_2,
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
    )

    # ETL Notification
    etl_notification.submit(
        success=file_upload_result.is_successful(),
        wait_for=[file_upload_result]
    )

if __name__ == '__main__':
    pcd_etl_pipeline()