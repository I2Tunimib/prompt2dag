from dagster import op, job, In, Out, Failure, RetryRequested, get_dagster_logger
from dagster_k8s import k8s_job_op
from dagster_utils import http_request, send_email  # Assume these are custom utility functions

logger = get_dagster_logger()

# Kubernetes job operators
check_pcd_sftp_folder = k8s_job_op.configured(
    name="check_pcd_sftp_folder",
    config={
        "namespace": "default",
        "job_name": "check-pcd-sftp-folder",
        "image": "my-k8s-image:latest",
        "command": ["python", "check_sftp.py"],
    }
)

check_pcd_shared_folder = k8s_job_op.configured(
    name="check_pcd_shared_folder",
    config={
        "namespace": "default",
        "job_name": "check-pcd-shared-folder",
        "image": "my-k8s-image:latest",
        "command": ["python", "check_shared.py"],
    }
)

pcd_file_upload = k8s_job_op.configured(
    name="pcd_file_upload",
    config={
        "namespace": "default",
        "job_name": "pcd-file-upload",
        "image": "my-k8s-image:latest",
        "command": ["python", "upload_file.py"],
    }
)

# Empty synchronization operators
@op
def start_pcd_extract_1():
    pass

@op
def start_pcd_extract_2():
    pass

# HTTP API extraction tasks
@op
def status_tracker():
    http_request("https://api.example.com/status_tracker")

@op
def financial_expense():
    http_request("https://api.example.com/financial_expense")

@op
def upcc_financial_reporting():
    http_request("https://api.example.com/upcc_financial_reporting")

@op
def chc_financial_reporting():
    http_request("https://api.example.com/chc_financial_reporting")

@op
def pcn_financial_reporting():
    http_request("https://api.example.com/pcn_financial_reporting")

@op
def nppcc_financial_reporting():
    http_request("https://api.example.com/nppcc_financial_reporting")

@op
def fiscal_year_reporting_dates():
    http_request("https://api.example.com/fiscal_year_reporting_dates")

@op
def upcc_primary_care_patient_services():
    http_request("https://api.example.com/upcc_primary_care_patient_services")

@op
def chc_primary_care_patient_services():
    http_request("https://api.example.com/chc_primary_care_patient_services")

@op
def practitioner_role_mapping():
    http_request("https://api.example.com/practitioner_role_mapping")

@op
def hr_records():
    http_request("https://api.example.com/hr_records")

@op
def provincial_risk_tracking():
    http_request("https://api.example.com/provincial_risk_tracking")

@op
def decision_log():
    http_request("https://api.example.com/decision_log")

@op
def ha_hierarchy():
    http_request("https://api.example.com/ha_hierarchy")

@op
def uppc_budget():
    http_request("https://api.example.com/uppc_budget")

@op
def chc_budget():
    http_request("https://api.example.com/chc_budget")

@op
def pcn_budget():
    http_request("https://api.example.com/pcn_budget")

@op
def nppcc_budget():
    http_request("https://api.example.com/nppcc_budget")

# Notification task
@op
def etl_notification(context, success: bool):
    subject = "ETL Pipeline Success" if success else "ETL Pipeline Failure"
    body = "The ETL pipeline has completed successfully." if success else "The ETL pipeline has failed."
    send_email(subject, body, "etl-team@example.com")

# Define the job
@job
def pcd_etl_pipeline():
    sftp_check = check_pcd_sftp_folder()
    shared_check = check_pcd_shared_folder(sftp_check)

    start_extract_1 = start_pcd_extract_1(shared_check)

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