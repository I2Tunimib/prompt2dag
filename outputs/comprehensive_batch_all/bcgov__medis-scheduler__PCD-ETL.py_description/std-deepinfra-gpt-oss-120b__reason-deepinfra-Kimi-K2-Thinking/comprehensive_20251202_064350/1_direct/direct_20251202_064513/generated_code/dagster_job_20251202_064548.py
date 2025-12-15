from dagster import op, job, In, Out, Nothing, get_dagster_logger


@op(out=Out(Nothing))
def check_pcd_sftp_folder():
    """Simulate a Kubernetes job that checks the SFTP folder for files."""
    logger = get_dagster_logger()
    logger.info("Checking PCD SFTP folder for files...")
    # Placeholder for actual Kubernetes job execution.
    return None


@op(ins={"previous": In(Nothing)}, out=Out(Nothing))
def check_pcd_shared_folder(previous):
    """Simulate a Kubernetes job that checks the shared folder for files."""
    logger = get_dagster_logger()
    logger.info("Checking PCD shared folder for files...")
    # Placeholder for actual Kubernetes job execution.
    return None


@op(ins={"previous": In(Nothing)}, out=Out(Nothing))
def start_pcd_extract_1(previous):
    """Synchronization point marking the start of parallel extraction."""
    logger = get_dagster_logger()
    logger.info("Starting parallel PCD extraction phase (Extract 1).")
    return None


def _dummy_extraction(name: str):
    """Factory to create simple extraction ops."""
    @op(ins={"start": In(Nothing)}, out=Out(dict))
    def extraction_op(start):
        logger = get_dagster_logger()
        logger.info(f"Extracting data for {name} via HTTP API...")
        # Placeholder for actual HTTP request logic.
        return {"name": name, "data": []}
    extraction_op.__name__ = f"{name.lower().replace(' ', '_')}"
    extraction_op.__doc__ = f"Extracts data for {name}."
    return extraction_op


status_tracker = _dummy_extraction("Status Tracker")
financial_expense = _dummy_extraction("Financial Expense")
upcc_financial_reporting = _dummy_extraction("UPCC Financial Reporting")
chc_financial_reporting = _dummy_extraction("CHC Financial Reporting")
pcn_financial_reporting = _dummy_extraction("PCN Financial Reporting")
nppcc_financial_reporting = _dummy_extraction("NPPCC Financial Reporting")
fiscal_year_reporting_dates = _dummy_extraction("Fiscal Year Reporting Dates")
upcc_primary_care_patient_services = _dummy_extraction("UPCC Primary Care Patient Services")
chc_primary_care_patient_services = _dummy_extraction("CHC Primary Care Patient Services")
practitioner_role_mapping = _dummy_extraction("Practitioner Role Mapping")
hr_records = _dummy_extraction("HR Records")
provincial_risk_tracking = _dummy_extraction("Provincial Risk Tracking")
decision_log = _dummy_extraction("Decision Log")
ha_hierarchy = _dummy_extraction("HA Hierarchy")
uppc_budget = _dummy_extraction("UPPC Budget")
chc_budget = _dummy_extraction("CHC Budget")
pcn_budget = _dummy_extraction("PCN Budget")
nppcc_budget = _dummy_extraction("NPPCC Budget")


@op(ins={"status_tracker": In(dict)}, out=Out(Nothing))
def start_pcd_extract_2(status_tracker):
    """Additional synchronization point after Status Tracker."""
    logger = get_dagster_logger()
    logger.info("Starting second extraction synchronization point (Extract 2).")
    return None


@op(
    ins={
        "status_tracker": In(dict),
        "financial_expense": In(dict),
        "upcc_financial_reporting": In(dict),
        "chc_financial_reporting": In(dict),
        "pcn_financial_reporting": In(dict),
        "nppcc_financial_reporting": In(dict),
        "fiscal_year_reporting_dates": In(dict),
        "upcc_primary_care_patient_services": In(dict),
        "chc_primary_care_patient_services": In(dict),
        "practitioner_role_mapping": In(dict),
        "hr_records": In(dict),
        "provincial_risk_tracking": In(dict),
        "decision_log": In(dict),
        "ha_hierarchy": In(dict),
        "uppc_budget": In(dict),
        "chc_budget": In(dict),
        "pcn_budget": In(dict),
        "nppcc_budget": In(dict),
        "extract_sync": In(Nothing),
    },
    out=Out(Nothing),
)
def pcd_file_upload(**kwargs):
    """Simulate a Kubernetes job that uploads the consolidated ETL file."""
    logger = get_dagster_logger()
    logger.info("Uploading consolidated PCD ETL file to destination...")
    # Placeholder for actual Kubernetes job execution.
    return None


@op(ins={"upload": In(Nothing)}, out=Out(Nothing))
def etl_notification(upload):
    """Send an email notification about the ETL run outcome."""
    logger = get_dagster_logger()
    logger.info("Sending ETL completion notification email...")
    # Placeholder for actual email sending logic.
    return None


@job
def pcd_etl_job():
    """Dagster job representing the PCD ETL pipeline."""
    sftp_check = check_pcd_sftp_folder()
    shared_check = check_pcd_shared_folder(sftp_check)
    extract_start_1 = start_pcd_extract_1(shared_check)

    # Parallel extraction tasks
    status = status_tracker(extract_start_1)
    financial = financial_expense(extract_start_1)
    upcc_fin = upcc_financial_reporting(extract_start_1)
    chc_fin = chc_financial_reporting(extract_start_1)
    pcn_fin = pcn_financial_reporting(extract_start_1)
    nppcc_fin = nppcc_financial_reporting(extract_start_1)
    fiscal_dates = fiscal_year_reporting_dates(extract_start_1)
    upcc_ps = upcc_primary_care_patient_services(extract_start_1)
    chc_ps = chc_primary_care_patient_services(extract_start_1)
    practitioner = practitioner_role_mapping(extract_start_1)
    hr = hr_records(extract_start_1)
    risk = provincial_risk_tracking(extract_start_1)
    decision = decision_log(extract_start_1)
    ha = ha_hierarchy(extract_start_1)
    uppc_bud = uppc_budget(extract_start_1)
    chc_bud = chc_budget(extract_start_1)
    pcn_bud = pcn_budget(extract_start_1)
    nppcc_bud = nppcc_budget(extract_start_1)

    extract_start_2 = start_pcd_extract_2(status)

    upload = pcd_file_upload(
        status_tracker=status,
        financial_expense=financial,
        upcc_financial_reporting=upcc_fin,
        chc_financial_reporting=chc_fin,
        pcn_financial_reporting=pcn_fin,
        nppcc_financial_reporting=nppcc_fin,
        fiscal_year_reporting_dates=fiscal_dates,
        upcc_primary_care_patient_services=upcc_ps,
        chc_primary_care_patient_services=chc_ps,
        practitioner_role_mapping=practitioner,
        hr_records=hr,
        provincial_risk_tracking=risk,
        decision_log=decision,
        ha_hierarchy=ha,
        uppc_budget=uppc_bud,
        chc_budget=chc_bud,
        pcn_budget=pcn_bud,
        nppcc_budget=nppcc_bud,
        extract_sync=extract_start_2,
    )

    etl_notification(upload)


if __name__ == "__main__":
    result = pcd_etl_job.execute_in_process()
    if result.success:
        print("PCD ETL job completed successfully.")
    else:
        print("PCD ETL job failed.")