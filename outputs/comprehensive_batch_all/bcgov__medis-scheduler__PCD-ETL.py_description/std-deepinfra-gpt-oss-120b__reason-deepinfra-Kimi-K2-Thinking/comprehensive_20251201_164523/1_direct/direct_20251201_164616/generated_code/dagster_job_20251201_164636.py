from __future__ import annotations

import time
from typing import Any, Dict, List

from dagster import In, Nothing, Out, JobDefinition, OpExecutionContext, job, op, get_dagster_logger


@op(out=Out(Nothing))
def check_pcd_sftp_folder(context: OpExecutionContext) -> None:
    """Simulate a Kubernetes job that verifies files in an SFTP folder."""
    logger = get_dagster_logger()
    logger.info("Checking PCD SFTP folder...")
    # Placeholder for actual Kubernetes job execution
    time.sleep(1)
    logger.info("SFTP folder check completed.")


@op(out=Out(Nothing), ins={"prev": In(Nothing)})
def check_pcd_shared_folder(context: OpExecutionContext, prev: Nothing) -> None:
    """Simulate a Kubernetes job that checks files in a shared directory."""
    logger = get_dagster_logger()
    logger.info("Checking PCD shared folder...")
    time.sleep(1)
    logger.info("Shared folder check completed.")


@op(out=Out(Nothing), ins={"prev": In(Nothing)})
def start_pcd_extract_1(context: OpExecutionContext, prev: Nothing) -> None:
    """Synchronization point before parallel extraction tasks."""
    logger = get_dagster_logger()
    logger.info("Starting parallel PCD extraction phase.")


def _simulate_api_call(name: str) -> Dict[str, Any]:
    """Utility to simulate an HTTP API extraction."""
    time.sleep(0.5)  # Simulate network latency
    return {"task": name, "status": "success", "data": []}


def _create_extraction_op(task_name: str):
    @op(out=Out(Dict[str, Any]), ins={"start": In(Nothing)})
    def extraction_op(context: OpExecutionContext, start: Nothing) -> Dict[str, Any]:
        logger = get_dagster_logger()
        logger.info(f"Extracting data for {task_name}...")
        result = _simulate_api_call(task_name)
        logger.info(f"{task_name} extraction completed.")
        return result

    extraction_op.__name__ = task_name.lower()
    extraction_op.__doc__ = f"Extract data from the {task_name} API."
    return extraction_op


# Parallel extraction ops
status_tracker = _create_extraction_op("Status_Tracker")
financial_expense = _create_extraction_op("Financial_Expense")
upcc_financial_reportingr = _create_extraction_op("UPCC_Financial_Reportingr")
chc_financial_reporting = _create_extraction_op("CHC_Financial_reporting")
pcn_financial_reporting = _create_extraction_op("PCN_Financial_Reporting")
nppcc_financial_reporting = _create_extraction_op("NPPCC_Financial_Reporting")
fiscal_year_reporting_dates = _create_extraction_op("Fiscal_Year_Reporting_Dates")
upcc_primary_care_patient_services = _create_extraction_op("UPCC_Primary_Care_Patient_Services")
chc_primary_care_patient_services = _create_extraction_op("CHC_Primary_Care_Patient_Services")
practitioner_role_mapping = _create_extraction_op("Practitioner_Role_Mapping")
hr_records = _create_extraction_op("HR_Records")
provincial_risk_tracking = _create_extraction_op("Provincial_Risk_Tracking")
decision_log = _create_extraction_op("Decision_Log")
ha_hierarchy = _create_extraction_op("HA_Hierarchy")
uppc_budget = _create_extraction_op("UPPC_Budget")
chc_budget = _create_extraction_op("CHC_Budget")
pcn_budget = _create_extraction_op("PCN_Budget")
nppcc_budget = _create_extraction_op("NPPCC_Budget")


@op(out=Out(Nothing), ins={"status_tracker": In(Dict[str, Any])})
def start_pcd_extract_2(context: OpExecutionContext, status_tracker: Dict[str, Any]) -> None:
    """Additional synchronization point after Status_Tracker."""
    logger = get_dagster_logger()
    logger.info("Start_PCD_Extract_2 triggered by Status_Tracker.")


@op(
    out=Out(Nothing),
    ins={
        "status_tracker": In(Dict[str, Any]),
        "financial_expense": In(Dict[str, Any]),
        "upcc_financial_reportingr": In(Dict[str, Any]),
        "chc_financial_reporting": In(Dict[str, Any]),
        "pcn_financial_reporting": In(Dict[str, Any]),
        "nppcc_financial_reporting": In(Dict[str, Any]),
        "fiscal_year_reporting_dates": In(Dict[str, Any]),
        "upcc_primary_care_patient_services": In(Dict[str, Any]),
        "chc_primary_care_patient_services": In(Dict[str, Any]),
        "practitioner_role_mapping": In(Dict[str, Any]),
        "hr_records": In(Dict[str, Any]),
        "provincial_risk_tracking": In(Dict[str, Any]),
        "decision_log": In(Dict[str, Any]),
        "ha_hierarchy": In(Dict[str, Any]),
        "uppc_budget": In(Dict[str, Any]),
        "chc_budget": In(Dict[str, Any]),
        "pcn_budget": In(Dict[str, Any]),
        "nppcc_budget": In(Dict[str, Any]),
        "sync_point": In(Nothing),
    },
)
def pcd_file_upload(context: OpExecutionContext, **kwargs: Any) -> None:
    """Simulate the main ETL file upload operation."""
    logger = get_dagster_logger()
    logger.info("Starting PCD file upload...")
    # In a real implementation, aggregate data from all extraction tasks.
    time.sleep(2)
    logger.info("PCD file upload completed successfully.")


@op(out=Out(Nothing), ins={"upload": In(Nothing)})
def etl_notification(context: OpExecutionContext, upload: Nothing) -> None:
    """Send an email notification based on the overall DAG run outcome."""
    logger = get_dagster_logger()
    run = context.instance.get_run_by_id(context.run_id)
    if run and run.status == "FAILURE":
        logger.info("ETL run failed. Sending failure notification email.")
    else:
        logger.info("ETL run succeeded. Sending success notification email.")
    # Placeholder for actual email sending logic.
    time.sleep(0.5)
    logger.info("Notification email dispatched.")


@job
def pcd_etl_job() -> JobDefinition:
    # Initial checks
    sftp_check = check_pcd_sftp_folder()
    shared_check = check_pcd_shared_folder(prev=sftp_check)

    # Synchronization before parallel extraction
    start_sync = start_pcd_extract_1(prev=shared_check)

    # Parallel extraction tasks
    st = status_tracker(start=start_sync)
    fe = financial_expense(start=start_sync)
    upcc_fr = upcc_financial_reportingr(start=start_sync)
    chc_fr = chc_financial_reporting(start=start_sync)
    pcn_fr = pcn_financial_reporting(start=start_sync)
    nppcc_fr = nppcc_financial_reporting(start=start_sync)
    fy_dates = fiscal_year_reporting_dates(start=start_sync)
    upcc_ps = upcc_primary_care_patient_services(start=start_sync)
    chc_ps = chc_primary_care_patient_services(start=start_sync)
    pr_map = practitioner_role_mapping(start=start_sync)
    hr = hr_records(start=start_sync)
    prisk = provincial_risk_tracking(start=start_sync)
    dlog = decision_log(start=start_sync)
    ha_h = ha_hierarchy(start=start_sync)
    uppc_b = uppc_budget(start=start_sync)
    chc_b = chc_budget(start=start_sync)
    pcn_b = pcn_budget(start=start_sync)
    nppcc_b = nppcc_budget(start=start_sync)

    # Second synchronization point
    sync_2 = start_pcd_extract_2(status_tracker=st)

    # File upload depends on all extraction tasks and the second sync point
    upload = pcd_file_upload(
        status_tracker=st,
        financial_expense=fe,
        upcc_financial_reportingr=upcc_fr,
        chc_financial_reporting=chc_fr,
        pcn_financial_reporting=pcn_fr,
        nppcc_financial_reporting=nppcc_fr,
        fiscal_year_reporting_dates=fy_dates,
        upcc_primary_care_patient_services=upcc_ps,
        chc_primary_care_patient_services=chc_ps,
        practitioner_role_mapping=pr_map,
        hr_records=hr,
        provincial_risk_tracking=prisk,
        decision_log=dlog,
        ha_hierarchy=ha_h,
        uppc_budget=uppc_b,
        chc_budget=chc_b,
        pcn_budget=pcn_b,
        nppcc_budget=nppcc_b,
        sync_point=sync_2,
    )

    # Notification runs after upload (regardless of upstream success/failure)
    etl_notification(upload=upload)

    return pcd_etl_job


if __name__ == "__main__":
    result = pcd_etl_job.execute_in_process()
    if result.success:
        print("PCD ETL job completed successfully.")
    else:
        print("PCD ETL job failed.")