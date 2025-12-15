from dagster import (
    op,
    job,
    ScheduleDefinition,
    Definitions,
    resource,
    RetryPolicy,
    Out,
    In,
    DefaultScheduleStatus,
)
import pandas as pd
from typing import Tuple
import io
import zipfile
from datetime import datetime


# Resource Stubs
@resource
def csv_data_source():
    """Stub for CSV file system resource."""
    class CSVDataSource:
        def read_csv(self, file_path: str) -> pd.DataFrame:
            # Minimal implementation - in real scenario would read from actual file system
            # Simulating a CSV with account data
            return pd.DataFrame({
                'account_id': ['123', '456', '789', '101'],
                'account_type': ['international', 'domestic', 'international', 'domestic'],
                'transaction_amount': [1000.0, 2000.0, 3000.0, 1500.0]
            })
    return CSVDataSource()


@resource
def fatca_reporting_system():
    """Stub for FATCA regulatory reporting system."""
    class FATCAReportingSystem:
        def generate_xml_report(self, data: pd.DataFrame) -> bytes:
            # Minimal implementation - would generate actual XML
            xml_content = f"<FATCAReport><record_count>{len(data)}</record_count><total_amount>{data['transaction_amount'].sum():.2f}</total_amount></FATCAReport>"
            return xml_content.encode('utf-8')
    return FATCAReportingSystem()


@resource
def irs_reporting_system():
    """Stub for IRS regulatory reporting system."""
    class IRSReportingSystem:
        def generate_form_1099_data(self, data: pd.DataFrame) -> bytes:
            # Minimal implementation - would generate actual Form 1099 data
            form_content = f"IRS1099|RECORDS:{len(data)}|TOTAL:{data['transaction_amount'].sum():.2f}"
            return form_content.encode('utf-8')
    return IRSReportingSystem()


@resource
def archive_storage():
    """Stub for secure archive storage system."""
    class ArchiveStorage:
        def store_archive(self, archive_data: bytes, archive_name: str) -> str:
            # Minimal implementation - would store to actual secure storage
            # For demo purposes, just return a success message
            return f"Successfully archived {archive_name} ({len(archive_data)} bytes)"
    return ArchiveStorage()


# Ops
@op(
    required_resource_keys={"csv_data_source"},
    retry_policy=RetryPolicy(max_retries=2, delay=300)  # 2 attempts, 5-minute delay
)
def read_csv(context) -> pd.DataFrame:
    """Reads financial transaction CSV file containing account type information."""
    # In production, use config to pass file path
    # file_path = context.op_config["file_path"]
    file_path = "financial_transactions.csv"
    context.log.info(f"Reading CSV from {file_path}")
    return context.resources.csv_data_source.read_csv(file_path)


@op(
    out={
        "international_accounts": Out(description="DataFrame containing international accounts"),
        "domestic_accounts": Out(description="DataFrame containing domestic accounts")
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def account_check(context, csv_data: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Analyzes account types and splits data for conditional routing."""
    context.log.info(f"Analyzing {len(csv_data)} total accounts")
    
    # Split based on account type
    international_mask = csv_data['account_type'] == 'international'
    international_accounts = csv_data[international_mask].copy()
    domestic_accounts = csv_data[~international_mask].copy()
    
    context.log.info(f"International accounts: {len(international_accounts)}")
    context.log.info(f"Domestic accounts: {len(domestic_accounts)}")
    
    return international_accounts, domestic_accounts


@op(
    required_resource_keys={"fatca_reporting_system"},
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def route_to_fatca(context, international_accounts: pd.DataFrame) -> bytes:
    """Processes international accounts through FATCA reporting system, generating XML reports."""
    if international_accounts.empty:
        context.log.info("No international accounts to process for FATCA")
        return b""
    
    context.log.info(f"Processing {len(international_accounts)} international accounts for FATCA reporting")
    return context.resources.fatca_reporting_system.generate_xml_report(international_accounts)


@op(
    required_resource_keys={"irs_reporting_system"},
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def route_to_irs(context, domestic_accounts: pd.DataFrame) -> bytes:
    """Processes domestic accounts through IRS reporting system, generating Form 1099 data."""
    if domestic_accounts.empty:
        context.log.info("No domestic accounts to process for IRS")
        return b""
    
    context.log.info(f"Processing {len(domestic_accounts)} domestic accounts for IRS reporting")
    return context.resources.irs_reporting_system.generate_form_1099_data(domestic_accounts)


@op(
    required_resource_keys={"archive_storage"},
    ins={
        "fatca_report": In(description="FATCA XML report data"),
        "irs_report": In(description="IRS Form 1099 data")
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)
def archive_reports(context, fatca_report: bytes, irs_report: bytes) -> str:
    """Merges outputs from both reporting branches, compresses reports, and stores in secure archive."""
    context.log.info("Merging and archiving regulatory reports")
    
    # Create compressed archive
    merged_data = io.BytesIO()
    with zipfile.ZipFile(merged_data, 'w', zipfile.ZIP_DEFLATED) as zipf:
        if fatca_report:
            zipf.writestr('fatca_report.xml', fatca_report)
        if irs_report:
            zipf.writestr('irs_1099_data.txt', irs_report)
    
    archive_data = merged_data.getvalue()
    archive_name = f"regulatory_reports_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.zip"
    
    context.log.info(f"Storing archive: {archive_name}")
    return context.resources.archive_storage.store_archive(archive_data, archive_name)


# Job
@job(
    resource_defs={
        "csv_data_source": csv_data_source,
        "fatca_reporting_system": fatca_reporting_system,
        "irs_reporting_system": irs_reporting_system,
        "archive_storage": archive_storage,
    },
    description="Regulatory report router pipeline with branch-merge pattern for financial transaction data"
)
def regulatory_report_router():
    """Defines the pipeline with conditional branching and parallel execution."""
    csv_data = read_csv()
    international_accounts, domestic_accounts = account_check(csv_data)
    
    # Parallel execution branches (max width: 2)
    fatca_result = route_to_fatca(international_accounts)
    irs_result = route_to_irs(domestic_accounts)
    
    # Synchronized merge
    archive_reports(fatca_result, irs_result)


# Schedule
regulatory_report_router_schedule = ScheduleDefinition(
    job=regulatory_report_router,
    cron_schedule="0 2 * * *",  # Daily at 2 AM UTC
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    # No catch-up for missed executions (Dagster default behavior)
)


# Definitions
defs = Definitions(
    jobs=[regulatory_report_router],
    schedules=[regulatory_report_router_schedule],
)


# Launch pattern
if __name__ == '__main__':
    # Execute the job directly
    result = regulatory_report_router.execute_in_process()
    print(f"Job execution {'succeeded' if result.success else 'failed'}")