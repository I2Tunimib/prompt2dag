from dagster import op, job, resource, RetryPolicy, Failure, Success, In, Out, Nothing
from dagster import execute_in_process
from typing import Dict, Any
import requests
import smtplib
from email.mime.text import MIMEText

# Resources
@resource(config_schema={"sftp_folder": str, "shared_folder": str, "k8s_job_template": str, "email_config": dict})
def pcd_resources(context):
    return {
        "sftp_folder": context.resource_config["sftp_folder"],
        "shared_folder": context.resource_config["shared_folder"],
        "k8s_job_template": context.resource_config["k8s_job_template"],
        "email_config": context.resource_config["email_config"],
    }

# Ops
@op(required_resource_keys={"pcd_resources"})
def check_pcd_sftp_folder(context):
    sftp_folder = context.resources.pcd_resources["sftp_folder"]
    # Simulate SFTP check
    context.log.info(f"Checking SFTP folder: {sftp_folder}")
    # Placeholder for actual SFTP check logic
    return True

@op(required_resource_keys={"pcd_resources"})
def check_pcd_shared_folder(context):
    shared_folder = context.resources.pcd_resources["shared_folder"]
    # Simulate shared folder check
    context.log.info(f"Checking shared folder: {shared_folder}")
    # Placeholder for actual shared folder check logic
    return True

@op
def start_pcd_extract_1(context):
    context.log.info("Starting parallel HTTP API extraction phase")

@op
def status_tracker(context):
    context.log.info("Tracking status")
    # Placeholder for actual status tracking logic
    return True

@op
def financial_expense(context):
    context.log.info("Extracting financial expense data")
    # Placeholder for actual HTTP API call
    return True

@op
def upcc_financial_reporting(context):
    context.log.info("Extracting UPCC financial reporting data")
    # Placeholder for actual HTTP API call
    return True

@op
def chc_financial_reporting(context):
    context.log.info("Extracting CHC financial reporting data")
    # Placeholder for actual HTTP API call
    return True

@op
def pcn_financial_reporting(context):
    context.log.info("Extracting PCN financial reporting data")
    # Placeholder for actual HTTP API call
    return True

@op
def nppcc_financial_reporting(context):
    context.log.info("Extracting NPPCC financial reporting data")
    # Placeholder for actual HTTP API call
    return True

@op
def fiscal_year_reporting_dates(context):
    context.log.info("Extracting fiscal year reporting dates")
    # Placeholder for actual HTTP API call
    return True

@op
def upcc_primary_care_patient_services(context):
    context.log.info("Extracting UPCC primary care patient services data")
    # Placeholder for actual HTTP API call
    return True

@op
def chc_primary_care_patient_services(context):
    context.log.info("Extracting CHC primary care patient services data")
    # Placeholder for actual HTTP API call
    return True

@op
def practitioner_role_mapping(context):
    context.log.info("Extracting practitioner role mapping data")
    # Placeholder for actual HTTP API call
    return True

@op
def hr_records(context):
    context.log.info("Extracting HR records")
    # Placeholder for actual HTTP API call
    return True

@op
def provincial_risk_tracking(context):
    context.log.info("Extracting provincial risk tracking data")
    # Placeholder for actual HTTP API call
    return True

@op
def decision_log(context):
    context.log.info("Extracting decision log data")
    # Placeholder for actual HTTP API call
    return True

@op
def ha_hierarchy(context):
    context.log.info("Extracting HA hierarchy data")
    # Placeholder for actual HTTP API call
    return True

@op
def uppc_budget(context):
    context.log.info("Extracting UPPC budget data")
    # Placeholder for actual HTTP API call
    return True

@op
def chc_budget(context):
    context.log.info("Extracting CHC budget data")
    # Placeholder for actual HTTP API call
    return True

@op
def pcn_budget(context):
    context.log.info("Extracting PCN budget data")
    # Placeholder for actual HTTP API call
    return True

@op
def nppcc_budget(context):
    context.log.info("Extracting NPPCC budget data")
    # Placeholder for actual HTTP API call
    return True

@op
def start_pcd_extract_2(context):
    context.log.info("Starting second phase of extraction")

@op(required_resource_keys={"pcd_resources"})
def pcd_file_upload(context):
    k8s_job_template = context.resources.pcd_resources["k8s_job_template"]
    # Simulate file upload
    context.log.info(f"Uploading file using Kubernetes job template: {k8s_job_template}")
    # Placeholder for actual file upload logic
    return True

@op(required_resource_keys={"pcd_resources"})
def etl_notification(context, success: bool):
    email_config = context.resources.pcd_resources["email_config"]
    subject = "ETL Pipeline Notification"
    body = "The ETL pipeline has completed successfully." if success else "The ETL pipeline has failed."
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = email_config["from"]
    msg["To"] = ", ".join(email_config["to"])

    with smtplib.SMTP(email_config["smtp_server"], email_config["smtp_port"]) as server:
        server.login(email_config["smtp_user"], email_config["smtp_password"])
        server.sendmail(email_config["from"], email_config["to"], msg.as_string())

# Job
@job(
    resource_defs={"pcd_resources": pcd_resources},
    tags={"timeout": "3600", "catchup": "False"},
)
def pcd_etl_pipeline():
    sftp_check = check_pcd_sftp_folder()
    shared_check = check_pcd_shared_folder(sftp_check)
    start_extract_1 = start_pcd_extract_1(shared_check)
    
    status_track = status_tracker(start_extract_1)
    financial_expense(start_extract_1)
    upcc_financial_reporting(start_extract_1)
    chc_financial_reporting(start_extract_1)
    pcn_financial_reporting(start_extract_1)
    nppcc_financial_reporting(start_extract_1)
    fiscal_year_reporting_dates(start_extract_1)
    upcc_primary_care_patient_services(start_extract_1)
    chc_primary_care_patient_services(start_extract_1)
    practitioner_role_mapping(start_extract_1)
    hr_records(start_extract_1)
    provincial_risk_tracking(start_extract_1)
    decision_log(start_extract_1)
    ha_hierarchy(start_extract_1)
    uppc_budget(start_extract_1)
    chc_budget(start_extract_1)
    pcn_budget(start_extract_1)
    nppcc_budget(start_extract_1)
    
    start_extract_2 = start_pcd_extract_2(status_track)
    
    file_upload = pcd_file_upload(start_extract_2)
    
    etl_notification(file_upload)

if __name__ == "__main__":
    result = pcd_etl_pipeline.execute_in_process()