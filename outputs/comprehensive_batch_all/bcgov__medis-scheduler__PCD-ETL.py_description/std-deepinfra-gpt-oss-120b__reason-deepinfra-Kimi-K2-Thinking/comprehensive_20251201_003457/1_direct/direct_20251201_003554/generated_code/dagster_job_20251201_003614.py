from dagster import op, job, In, Out, Nothing, get_dagster_logger


@op(description="Verifies the presence of files in the designated SFTP folder.")
def check_pcd_sftp_folder() -> Nothing:
    logger = get_dagster_logger()
    logger.info("Checking PCD SFTP folder for files...")
    # Placeholder for actual Kubernetes job execution or SFTP check logic.
    return None


@op(description="Checks for files in the shared directory.")
def check_pcd_shared_folder() -> Nothing:
    logger = get_dagster_logger()
    logger.info("Checking PCD shared folder for files...")
    # Placeholder for actual Kubernetes job execution or shared folder check logic.
    return None


@op(description="Synchronization point marking the start of parallel API extraction.")
def start_pcd_extract_1() -> Nothing:
    logger = get_dagster_logger()
    logger.info("Starting parallel PCD extraction phase.")
    return None


def _http_api_op(name: str):
    @op(name=name, description=f"Extracts data from the {name} HTTP API endpoint.")
    def _op() -> Nothing:
        logger = get_dagster_logger()
        logger.info(f"Fetching data from {name} API...")
        # Placeholder for actual HTTP request logic.
        return None

    return _op


# Parallel extraction ops
status_tracker = _http_api_op("status_tracker")
financial_expense = _http_api_op("financial_expense")
upcc_financial_reporting = _http_api_op("upcc_financial_reporting")
chc_financial_reporting = _http_api_op("chc_financial_reporting")
pcn_financial_reporting = _http_api_op("pcn_financial_reporting")
nppcc_financial_reporting = _http_api_op("nppcc_financial_reporting")
fiscal_year_reporting_dates = _http_api_op("fiscal_year_reporting_dates")
upcc_primary_care_patient_services = _http_api_op("upcc_primary_care_patient_services")
chc_primary_care_patient_services = _http_api_op("chc_primary_care_patient_services")
practitioner_role_mapping = _http_api_op("practitioner_role_mapping")
hr_records = _http_api_op("hr_records")
provincial_risk_tracking = _http_api_op("provincial_risk_tracking")
decision_log = _http_api_op("decision_log")
ha_hierarchy = _http_api_op("ha_hierarchy")
uppc_budget = _http_api_op("uppc_budget")
chc_budget = _http_api_op("chc_budget")
pcn_budget = _http_api_op("pcn_budget")
nppcc_budget = _http_api_op("nppcc_budget")


@op(description="Additional synchronization point triggered after Status_Tracker.")
def start_pcd_extract_2() -> Nothing:
    logger = get_dagster_logger()
    logger.info("Starting second synchronization point after Status_Tracker.")
    return None


@op(description="Performs the main ETL file upload operation.")
def pcd_file_upload() -> Nothing:
    logger = get_dagster_logger()
    logger.info("Uploading consolidated PCD files...")
    # Placeholder for actual Kubernetes job execution or file upload logic.
    return None


@op(description="Sends an email notification about the ETL run outcome.")
def etl_notification() -> Nothing:
    logger = get_dagster_logger()
    logger.info("Sending ETL completion notification email...")
    # Placeholder for email sending logic.
    return None


@job(description="PCD ETL pipeline orchestrating checks, parallel API extraction, file upload, and notification.")
def pcd_etl_job():
    # Initial folder checks
    sftp_check = check_pcd_sftp_folder()
    shared_check = check_pcd_shared_folder()
    sftp_check >> shared_check

    # Start of parallel extraction
    start1 = start_pcd_extract_1()
    shared_check >> start1

    # Parallel extraction tasks
    extraction_ops = [
        status_tracker(),
        financial_expense(),
        upcc_financial_reporting(),
        chc_financial_reporting(),
        pcn_financial_reporting(),
        nppcc_financial_reporting(),
        fiscal_year_reporting_dates(),
        upcc_primary_care_patient_services(),
        chc_primary_care_patient_services(),
        practitioner_role_mapping(),
        hr_records(),
        provincial_risk_tracking(),
        decision_log(),
        ha_hierarchy(),
        uppc_budget(),
        chc_budget(),
        pcn_budget(),
        nppcc_budget(),
    ]

    # All extraction ops depend on the start signal
    start1 >> extraction_ops

    # Second synchronization point after Status_Tracker
    start2 = start_pcd_extract_2()
    status_tracker() >> start2

    # File upload depends on all extraction ops and the second sync point
    upload = pcd_file_upload()
    extraction_ops + [start2] >> upload

    # Notification runs after the upload step
    notification = etl_notification()
    upload >> notification


if __name__ == "__main__":
    result = pcd_etl_job.execute_in_process()
    if result.success:
        print("PCD ETL job completed successfully.")
    else:
        print("PCD ETL job failed.")