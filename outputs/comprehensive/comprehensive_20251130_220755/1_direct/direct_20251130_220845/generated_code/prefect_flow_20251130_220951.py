from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_email import EmailNotify
from prefect_kubernetes import KubernetesJob

# Task to check SFTP folder for files
@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def check_pcd_sftp_folder():
    logger = get_run_logger()
    logger.info("Checking PCD SFTP folder for files.")
    # Kubernetes job operator to check SFTP folder
    k8s_job = KubernetesJob(
        job="check-sftp-folder-job.yaml",
        namespace="default"
    )
    k8s_job.run()
    return True

# Task to check shared folder for files
@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def check_pcd_shared_folder(sftp_check_result):
    logger = get_run_logger()
    logger.info("Checking PCD shared folder for files.")
    # Kubernetes job operator to check shared folder
    k8s_job = KubernetesJob(
        job="check-shared-folder-job.yaml",
        namespace="default"
    )
    k8s_job.run()
    return True

# Task to start parallel API extraction
@task
def start_pcd_extract_1():
    logger = get_run_logger()
    logger.info("Starting parallel API extraction phase.")
    return True

# Parallel HTTP API extraction tasks
@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def status_tracker():
    logger = get_run_logger()
    logger.info("Extracting status tracker data.")
    # Simulate API call
    return True

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def financial_expense():
    logger = get_run_logger()
    logger.info("Extracting financial expense data.")
    # Simulate API call
    return True

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def upcc_financial_reporting():
    logger = get_run_logger()
    logger.info("Extracting UPCC financial reporting data.")
    # Simulate API call
    return True

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def chc_financial_reporting():
    logger = get_run_logger()
    logger.info("Extracting CHC financial reporting data.")
    # Simulate API call
    return True

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def pcn_financial_reporting():
    logger = get_run_logger()
    logger.info("Extracting PCN financial reporting data.")
    # Simulate API call
    return True

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def nppcc_financial_reporting():
    logger = get_run_logger()
    logger.info("Extracting NPPCC financial reporting data.")
    # Simulate API call
    return True

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def fiscal_year_reporting_dates():
    logger = get_run_logger()
    logger.info("Extracting fiscal year reporting dates.")
    # Simulate API call
    return True

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def upcc_primary_care_patient_services():
    logger = get_run_logger()
    logger.info("Extracting UPCC primary care patient services data.")
    # Simulate API call
    return True

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def chc_primary_care_patient_services():
    logger = get_run_logger()
    logger.info("Extracting CHC primary care patient services data.")
    # Simulate API call
    return True

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def practitioner_role_mapping():
    logger = get_run_logger()
    logger.info("Extracting practitioner role mapping data.")
    # Simulate API call
    return True

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def hr_records():
    logger = get_run_logger()
    logger.info("Extracting HR records data.")
    # Simulate API call
    return True

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def provincial_risk_tracking():
    logger = get_run_logger()
    logger.info("Extracting provincial risk tracking data.")
    # Simulate API call
    return True

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def decision_log():
    logger = get_run_logger()
    logger.info("Extracting decision log data.")
    # Simulate API call
    return True

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def ha_hierarchy():
    logger = get_run_logger()
    logger.info("Extracting HA hierarchy data.")
    # Simulate API call
    return True

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def uppc_budget():
    logger = get_run_logger()
    logger.info("Extracting UPPC budget data.")
    # Simulate API call
    return True

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def chc_budget():
    logger = get_run_logger()
    logger.info("Extracting CHC budget data.")
    # Simulate API call
    return True

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def pcn_budget():
    logger = get_run_logger()
    logger.info("Extracting PCN budget data.")
    # Simulate API call
    return True

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def nppcc_budget():
    logger = get_run_logger()
    logger.info("Extracting NPPCC budget data.")
    # Simulate API call
    return True

# Task to start second synchronization point
@task
def start_pcd_extract_2(status_tracker_result):
    logger = get_run_logger()
    logger.info("Starting second synchronization point.")
    return True

# Task to perform file upload
@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def pcd_file_upload():
    logger = get_run_logger()
    logger.info("Performing PCD file upload.")
    # Kubernetes job operator to upload file
    k8s_job = KubernetesJob(
        job="file-upload-job.yaml",
        namespace="default"
    )
    k8s_job.run()
    return True

# Task to send notification
@task
def etl_notification(extraction_results):
    logger = get_run_logger()
    logger.info("Sending ETL notification.")
    # Determine success or failure
    all_success = all(extraction_results)
    if all_success:
        subject = "ETL Pipeline Success"
        message = "The ETL pipeline has completed successfully."
    else:
        subject = "ETL Pipeline Failure"
        message = "The ETL pipeline has encountered failures."

    # Send email notification
    email = EmailNotify(
        subject=subject,
        email_to="example@example.com",
        message=message
    )
    email.run()
    return True

# Main flow
@flow(timeout_seconds=3600)
def pcd_etl_pipeline():
    logger = get_run_logger()
    logger.info("Starting PCD ETL pipeline.")

    # Check SFTP folder
    sftp_check_result = check_pcd_sftp_folder()

    # Check shared folder
    shared_folder_check_result = check_pcd_shared_folder(sftp_check_result)

    # Start parallel API extraction
    start_extract_1_result = start_pcd_extract_1()

    # Parallel API extraction tasks
    status_tracker_result = status_tracker.submit(start_extract_1_result)
    financial_expense_result = financial_expense.submit(start_extract_1_result)
    upcc_financial_reporting_result = upcc_financial_reporting.submit(start_extract_1_result)
    chc_financial_reporting_result = chc_financial_reporting.submit(start_extract_1_result)
    pcn_financial_reporting_result = pcn_financial_reporting.submit(start_extract_1_result)
    nppcc_financial_reporting_result = nppcc_financial_reporting.submit(start_extract_1_result)
    fiscal_year_reporting_dates_result = fiscal_year_reporting_dates.submit(start_extract_1_result)
    upcc_primary_care_patient_services_result = upcc_primary_care_patient_services.submit(start_extract_1_result)
    chc_primary_care_patient_services_result = chc_primary_care_patient_services.submit(start_extract_1_result)
    practitioner_role_mapping_result = practitioner_role_mapping.submit(start_extract_1_result)
    hr_records_result = hr_records.submit(start_extract_1_result)
    provincial_risk_tracking_result = provincial_risk_tracking.submit(start_extract_1_result)
    decision_log_result = decision_log.submit(start_extract_1_result)
    ha_hierarchy_result = ha_hierarchy.submit(start_extract_1_result)
    uppc_budget_result = uppc_budget.submit(start_extract_1_result)
    chc_budget_result = chc_budget.submit(start_extract_1_result)
    pcn_budget_result = pcn_budget.submit(start_extract_1_result)
    nppcc_budget_result = nppcc_budget.submit(start_extract_1_result)

    # Start second synchronization point
    start_extract_2_result = start_pcd_extract_2(status_tracker_result)

    # Perform file upload
    file_upload_result = pcd_file_upload.submit(
        start_extract_2_result,
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
    )

    # Send notification
    etl_notification.submit(
        [
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
            nppcc_budget_result,
            file_upload_result
        ]
    )

if __name__ == "__main__":
    pcd_etl_pipeline()