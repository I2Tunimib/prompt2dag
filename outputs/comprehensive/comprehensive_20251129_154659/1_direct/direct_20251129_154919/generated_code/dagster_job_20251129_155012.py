from dagster import op, job, resource, RetryPolicy, Failure, Success, In, Out, Nothing

# Resources
@resource(config_schema={"sftp_folder": str, "shared_folder": str})
def folder_checker_resource(context):
    return {
        "sftp_folder": context.resource_config["sftp_folder"],
        "shared_folder": context.resource_config["shared_folder"],
    }

@resource(config_schema={"email_recipients": str})
def email_notification_resource(context):
    return context.resource_config["email_recipients"]

# Ops
@op(required_resource_keys={"folder_checker"})
def check_pcd_sftp_folder(context):
    """Check for files in the SFTP folder."""
    sftp_folder = context.resources.folder_checker["sftp_folder"]
    # Simulate folder check
    if not sftp_folder:
        raise Failure("No files found in SFTP folder")
    context.log.info(f"Files found in SFTP folder: {sftp_folder}")

@op(required_resource_keys={"folder_checker"})
def check_pcd_shared_folder(context):
    """Check for files in the shared folder."""
    shared_folder = context.resources.folder_checker["shared_folder"]
    # Simulate folder check
    if not shared_folder:
        raise Failure("No files found in shared folder")
    context.log.info(f"Files found in shared folder: {shared_folder}")

@op
def start_pcd_extract_1(context):
    """Synchronization point for parallel API extraction phase."""
    context.log.info("Starting parallel API extraction phase")

@op
def status_tracker(context):
    """Track the status of the pipeline."""
    context.log.info("Tracking pipeline status")

@op
def financial_expense(context):
    """Extract financial expense data."""
    context.log.info("Extracting financial expense data")

@op
def upcc_financial_reporting(context):
    """Extract UPCC financial reporting data."""
    context.log.info("Extracting UPCC financial reporting data")

@op
def chc_financial_reporting(context):
    """Extract CHC financial reporting data."""
    context.log.info("Extracting CHC financial reporting data")

@op
def pcn_financial_reporting(context):
    """Extract PCN financial reporting data."""
    context.log.info("Extracting PCN financial reporting data")

@op
def nppcc_financial_reporting(context):
    """Extract NPPCC financial reporting data."""
    context.log.info("Extracting NPPCC financial reporting data")

@op
def fiscal_year_reporting_dates(context):
    """Extract fiscal year reporting dates."""
    context.log.info("Extracting fiscal year reporting dates")

@op
def upcc_primary_care_patient_services(context):
    """Extract UPCC primary care patient services data."""
    context.log.info("Extracting UPCC primary care patient services data")

@op
def chc_primary_care_patient_services(context):
    """Extract CHC primary care patient services data."""
    context.log.info("Extracting CHC primary care patient services data")

@op
def practitioner_role_mapping(context):
    """Extract practitioner role mapping data."""
    context.log.info("Extracting practitioner role mapping data")

@op
def hr_records(context):
    """Extract HR records."""
    context.log.info("Extracting HR records")

@op
def provincial_risk_tracking(context):
    """Extract provincial risk tracking data."""
    context.log.info("Extracting provincial risk tracking data")

@op
def decision_log(context):
    """Extract decision log data."""
    context.log.info("Extracting decision log data")

@op
def ha_hierarchy(context):
    """Extract HA hierarchy data."""
    context.log.info("Extracting HA hierarchy data")

@op
def uppc_budget(context):
    """Extract UPPC budget data."""
    context.log.info("Extracting UPPC budget data")

@op
def chc_budget(context):
    """Extract CHC budget data."""
    context.log.info("Extracting CHC budget data")

@op
def pcn_budget(context):
    """Extract PCN budget data."""
    context.log.info("Extracting PCN budget data")

@op
def nppcc_budget(context):
    """Extract NPPCC budget data."""
    context.log.info("Extracting NPPCC budget data")

@op
def start_pcd_extract_2(context):
    """Synchronization point after status tracking."""
    context.log.info("Starting second extraction phase")

@op
def pcd_file_upload(context):
    """Upload the consolidated file to the target system."""
    context.log.info("Uploading consolidated file")

@op(required_resource_keys={"email_notification"})
def etl_notification(context, success: bool):
    """Send an email notification with the final status of the pipeline."""
    recipients = context.resources.email_notification["email_recipients"]
    if success:
        context.log.info(f"Sending success email to {recipients}")
    else:
        context.log.info(f"Sending failure email to {recipients}")

# Job
@job(
    resource_defs={
        "folder_checker": folder_checker_resource,
        "email_notification": email_notification_resource,
    },
    tags={"timeout": "3600", "catchup": "False"},
)
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
    
    file_upload_task = pcd_file_upload(
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
        nppcc_budget_task,
    )
    
    etl_notification(file_upload_task)

if __name__ == "__main__":
    result = pcd_etl_pipeline.execute_in_process()