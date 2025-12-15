from dagster import op, job, RetryPolicy, Failure, Success, In, Out, Nothing, get_dagster_logger
from dagster_k8s import k8s_job_op
from dagster_utils import http_request, send_email

# Simplified resource or config examples
# KUBERNETES_JOB_CONFIG = {
#     'namespace': 'default',
#     'job_name': 'pcd-file-upload',
#     'image': 'my-image:latest',
#     'command': ['python', 'upload.py']
# }

# AIRFLOW_VARIABLES = {
#     'pcd_etl_schedule': '0 0 * * *',  # Cron expression
#     'email_recipients': 'user@example.com',
#     'api_endpoints': {
#         'status_tracker': 'http://status-tracker-api.com',
#         'financial_expense': 'http://financial-expense-api.com',
#         # Add other API endpoints here
#     }
# }

@op
def check_pcd_sftp_folder(context):
    """Check for files in the PCD SFTP folder."""
    # Example: Use a Kubernetes job to check the SFTP folder
    # k8s_job_op(context, KUBERNETES_JOB_CONFIG)
    context.log.info("Checked PCD SFTP folder")

@op
def check_pcd_shared_folder(context):
    """Check for files in the PCD shared folder."""
    # Example: Use a Kubernetes job to check the shared folder
    # k8s_job_op(context, KUBERNETES_JOB_CONFIG)
    context.log.info("Checked PCD shared folder")

@op
def start_pcd_extract_1(context):
    """Synchronization point for the start of parallel API extraction."""
    context.log.info("Starting parallel API extraction")

@op
def status_tracker(context):
    """Extract data from the status tracker API."""
    # Example: Make an HTTP request to the status tracker API
    # response = http_request(AIRFLOW_VARIABLES['api_endpoints']['status_tracker'])
    context.log.info("Extracted data from status tracker API")

@op
def financial_expense(context):
    """Extract financial expense data."""
    # Example: Make an HTTP request to the financial expense API
    # response = http_request(AIRFLOW_VARIABLES['api_endpoints']['financial_expense'])
    context.log.info("Extracted financial expense data")

# Define other parallel extraction tasks similarly
@op
def upcc_financial_reporting(context):
    context.log.info("Extracted UPCC financial reporting data")

@op
def chc_financial_reporting(context):
    context.log.info("Extracted CHC financial reporting data")

@op
def pcn_financial_reporting(context):
    context.log.info("Extracted PCN financial reporting data")

@op
def nppcc_financial_reporting(context):
    context.log.info("Extracted NPPCC financial reporting data")

@op
def fiscal_year_reporting_dates(context):
    context.log.info("Extracted fiscal year reporting dates")

@op
def upcc_primary_care_patient_services(context):
    context.log.info("Extracted UPCC primary care patient services data")

@op
def chc_primary_care_patient_services(context):
    context.log.info("Extracted CHC primary care patient services data")

@op
def practitioner_role_mapping(context):
    context.log.info("Extracted practitioner role mapping data")

@op
def hr_records(context):
    context.log.info("Extracted HR records data")

@op
def provincial_risk_tracking(context):
    context.log.info("Extracted provincial risk tracking data")

@op
def decision_log(context):
    context.log.info("Extracted decision log data")

@op
def ha_hierarchy(context):
    context.log.info("Extracted HA hierarchy data")

@op
def uppc_budget(context):
    context.log.info("Extracted UPPC budget data")

@op
def chc_budget(context):
    context.log.info("Extracted CHC budget data")

@op
def pcn_budget(context):
    context.log.info("Extracted PCN budget data")

@op
def nppcc_budget(context):
    context.log.info("Extracted NPPCC budget data")

@op
def start_pcd_extract_2(context):
    """Synchronization point after the status tracker task."""
    context.log.info("Starting second phase of extraction")

@op
def pcd_file_upload(context):
    """Upload the consolidated file using a Kubernetes job."""
    # Example: Use a Kubernetes job to upload the file
    # k8s_job_op(context, KUBERNETES_JOB_CONFIG)
    context.log.info("Uploaded PCD file")

@op
def etl_notification(context, success: bool):
    """Send an email notification with the final status of the ETL run."""
    # Example: Send an email using Airflow's email backend
    # send_email(AIRFLOW_VARIABLES['email_recipients'], "ETL Run Status", f"ETL run {'succeeded' if success else 'failed'}")
    context.log.info(f"Sent ETL notification: {'Success' if success else 'Failure'}")

@job(
    resource_defs={
        # Define resources here if needed
    },
    tags={
        'dagster-k8s/config': {
            'container_config': {
                'resources': {
                    'requests': {'cpu': '100m', 'memory': '256Mi'},
                    'limits': {'cpu': '500m', 'memory': '512Mi'}
                }
            }
        }
    },
    description="ETL pipeline for PCD (Primary Care Data) that orchestrates data extraction from multiple HTTP API endpoints and executes a file processing job on Kubernetes."
)
def pcd_etl_pipeline():
    check_sftp = check_pcd_sftp_folder()
    check_shared = check_pcd_shared_folder(check_sftp)
    start_extract_1 = start_pcd_extract_1(check_shared)

    status_tracker_task = status_tracker(start_extract_1)
    financial_expense_task = financial_expense(start_extract_1)
    upcc_financial_reporting_task = upcc_financial_reporting(start_extract_1)
    chc_financial_reporting_task = chc_financial_reporting(start_extract_1)
    pcn_financial_reporting_task = pcn_financial_reporting(start_extract_1)
    nppcc_financial_reporting_task = nppcc_financial_reporting(start_extract_1)
    fiscal_year_reporting_dates_task = fiscal_year_reporting_dates(start_extract_1)
    upcc_primary_care_patient_services_task = upcc_primary_care_patient_services(start_extract_1)
    chc_primary_care_patient_services_task = chc_primary_care_patient_services(start_extract_1)
    practitioner_role_mapping_task = practitioner_role_mapping(start_extract_1)
    hr_records_task = hr_records(start_extract_1)
    provincial_risk_tracking_task = provincial_risk_tracking(start_extract_1)
    decision_log_task = decision_log(start_extract_1)
    ha_hierarchy_task = ha_hierarchy(start_extract_1)
    uppc_budget_task = uppc_budget(start_extract_1)
    chc_budget_task = chc_budget(start_extract_1)
    pcn_budget_task = pcn_budget(start_extract_1)
    nppcc_budget_task = nppcc_budget(start_extract_1)

    start_extract_2 = start_pcd_extract_2(status_tracker_task)
    file_upload = pcd_file_upload(
        start_extract_2,
        status_tracker_task,
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

    etl_notification(file_upload)

if __name__ == '__main__':
    result = pcd_etl_pipeline.execute_in_process()