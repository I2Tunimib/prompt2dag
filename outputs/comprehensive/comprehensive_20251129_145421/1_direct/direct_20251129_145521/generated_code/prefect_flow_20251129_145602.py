from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from prefect.kubernetes import KubernetesJob
from prefect.tasks.notifications import EmailNotification
import json

# Task definitions
@task(cache_key_fn=task_input_hash)
def check_pcd_sftp_folder():
    logger = get_run_logger()
    logger.info("Checking PCD SFTP folder...")
    # Simulate Kubernetes job execution
    job_template_file = '{{var.value.pcd_emtysftp_job}}'
    job = KubernetesJob(job_template_file=job_template_file, wait_until_job_complete=True)
    job.run()
    return "SFTP folder check completed successfully"

@task(cache_key_fn=task_input_hash)
def check_pcd_shared_folder(sftp_check_result):
    logger = get_run_logger()
    logger.info("Checking PCD shared folder...")
    # Simulate Kubernetes job execution
    job_template_file = '{{var.value.pcd_emtydir_job}}'
    job = KubernetesJob(job_template_file=job_template_file, wait_until_job_complete=True)
    job.run()
    return "Shared folder check completed successfully"

@task(cache_key_fn=task_input_hash)
def start_pcd_extract_1(shared_folder_check_result):
    logger = get_run_logger()
    logger.info("Starting PCD extract 1...")
    return "PCD extract 1 started"

@task(cache_key_fn=task_input_hash)
def extract_api_data(api_name, api_url):
    logger = get_run_logger()
    logger.info(f"Extracting data from {api_name} API...")
    # Simulate HTTP POST request
    response = {"status_code": 200, "data": "Sample data"}
    if response["status_code"] != 200:
        raise ValueError(f"API {api_name} returned status code {response['status_code']}")
    return response

@task(cache_key_fn=task_input_hash)
def start_pcd_extract_2(status_tracker_result):
    logger = get_run_logger()
    logger.info("Starting PCD extract 2...")
    return "PCD extract 2 started"

@task(cache_key_fn=task_input_hash)
def pcd_file_upload(extract_results):
    logger = get_run_logger()
    logger.info("Uploading PCD files...")
    # Simulate Kubernetes job execution
    job_template_file = '{{var.value.pcd_job}}'
    job = KubernetesJob(job_template_file=job_template_file)
    job.run()
    return "PCD files uploaded successfully"

@task(cache_key_fn=task_input_hash)
def etl_notification(success, failed_tasks):
    logger = get_run_logger()
    logger.info("Sending ETL notification...")
    # Simulate email notification
    if success:
        recipients = '{{var.value.PCD_ETL_email_list_success}}'
        subject = "ETL Pipeline Success"
        body = "The ETL pipeline completed successfully."
    else:
        recipients = '{{var.value.ETL_email_list_alerts}}'
        subject = "ETL Pipeline Failure"
        body = f"The ETL pipeline failed. Failed tasks: {', '.join(failed_tasks)}"
    
    email = EmailNotification(recipients=recipients, subject=subject, body=body)
    email.run()
    return "Notification sent"

# Flow definition
@flow(name="PCD_ETL_Pipeline")
def pcd_etl_pipeline():
    logger = get_run_logger()
    logger.info("Starting PCD ETL Pipeline...")

    # Sequential folder checks
    sftp_check_result = check_pcd_sftp_folder()
    shared_folder_check_result = check_pcd_shared_folder(sftp_check_result)

    # Synchronization point
    extract_1_start = start_pcd_extract_1(shared_folder_check_result)

    # Parallel HTTP API extraction
    api_names = [
        "Financial_Expense", "UPCC_Financial_Reporting", "CHC_Financial_reporting", "PCN_Financial_Reporting",
        "NPPCC_Financial_Reporting", "Fiscal_Year_Reporting_Dates", "UPCC_Primary_Care_Patient_Services",
        "CHC_Primary_Care_Patient_Services", "Practitioner_Role_Mapping", "Status_Tracker", "HR_Records",
        "Provincial_Risk_Tracking", "Decision_Log", "HA_Hierarchy", "UPPC_Budget", "CHC_Budget", "PCN_Budget",
        "NPPCC_Budget"
    ]
    api_urls = [f'{{var.value.pcd_{api_name.lower()}_url}}' for api_name in api_names]

    extract_results = []
    for api_name, api_url in zip(api_names, api_urls):
        extract_result = extract_api_data.submit(api_name, api_url)
        extract_results.append(extract_result)

    # Synchronization point
    status_tracker_result = extract_api_data.submit("Status_Tracker", '{{var.value.pcd_status_tracker_url}}')
    extract_2_start = start_pcd_extract_2(status_tracker_result)

    # Sequential data processing and upload
    upload_result = pcd_file_upload(extract_results)

    # Notification
    try:
        etl_notification(True, [])
    except Exception as e:
        logger.error(f"ETL pipeline failed: {e}")
        etl_notification(False, [str(e)])

if __name__ == '__main__':
    pcd_etl_pipeline()