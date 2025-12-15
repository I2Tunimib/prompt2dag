from dagster import (
    op,
    job,
    ScheduleDefinition,
    Definitions,
    ResourceDefinition,
    RetryPolicy,
    Out,
    Config,
    OpExecutionContext,
)
from typing import List, Dict, Any
from datetime import datetime


# Minimal resource stubs
class CSVFileSystem:
    """Stub for CSV file system operations."""
    
    def read_csv(self, file_path: str) -> List[Dict[str, Any]]:
        """Simulate reading a CSV file."""
        # In production, implement actual CSV reading logic
        return [
            {"account_id": "INT001", "account_type": "international", "amount": 10000},
            {"account_id": "DOM001", "account_type": "domestic", "amount": 5000},
            {"account_id": "INT002", "account_type": "international", "amount": 25000},
        ]


class FATCAReportingSystem:
    """Stub for FATCA regulatory reporting system."""
    
    def generate_xml_report(self, transactions: List[Dict[str, Any]]) -> str:
        """Simulate generating FATCA XML report."""
        # In production, implement actual XML generation
        return f"<FATCAReport><transaction_count>{len(transactions)}</transaction_count></FATCAReport>"


class IRSReportingSystem:
    """Stub for IRS regulatory reporting system."""
    
    def generate_form_1099(self, transactions: List[Dict[str, Any]]) -> str:
        """Simulate generating IRS Form 1099 data."""
        # In production, implement actual Form 1099 generation
        total_amount = sum(t.get("amount", 0) for t in transactions)
        return f"IRS1099|count:{len(transactions)}|total:{total_amount}"


class SecureArchiveStorage:
    """Stub for secure archive storage system."""
    
    def archive_reports(self, fatca_data: str, irs_data: str, location: str) -> str:
        """Simulate archiving reports."""
        # In production, implement actual compression and archival
        timestamp = datetime.now().isoformat()
        return f"ARCHIVE|ts:{timestamp}|FATCA:{len(fatca_data)}|IRS:{len(irs_data)}|loc:{location}"


# Resource definitions
csv_file_system = ResourceDefinition.hardcoded_resource(CSVFileSystem())
fatca_reporting_system = ResourceDefinition.hardcoded_resource(FATCAReportingSystem())
irs_reporting_system = ResourceDefinition.hardcoded_resource(IRSReportingSystem())
secure_archive_storage = ResourceDefinition.hardcoded_resource(SecureArchiveStorage())


# Config classes
class ReadCSVConfig(Config):
    """Configuration for reading CSV file."""
    csv_path: str = "financial_transactions.csv"


class ArchiveConfig(Config):
    """Configuration for archiving reports."""
    archive_location: str = "/secure/regulatory_archive"


# Retry policy: 2 attempts total, 5-minute delay
retry_policy = RetryPolicy(max_retries=1, delay=300)


@op(
    retry_policy=retry_policy,
    config_schema=ReadCSVConfig,
)
def read_csv(context: OpExecutionContext, config: ReadCSVConfig) -> List[Dict[str, Any]]:
    """Reads financial transaction CSV file containing account type information."""
    csv_resource = context.resources.csv_source
    context.log.info(f"Reading CSV from {config.csv_path}")
    transactions = csv_resource.read_csv(config.csv_path)
    context.log.info(f"Successfully read {len(transactions)} transactions")
    return transactions


@op(
    out={
        "international_accounts": Out(description="International account transactions"),
        "domestic_accounts": Out(description="Domestic account transactions"),
    },
    retry_policy=retry_policy,
)
def account_check(context: OpExecutionContext, transactions: List[Dict[str, Any]]):
    """Analyzes account types and determines routing path using conditional branching."""
    international = [
        t for t in transactions 
        if t.get("account_type") == "international"
    ]
    domestic = [
        t for t in transactions 
        if t.get("account_type") == "domestic"
    ]
    
    context.log.info(
        f"Routing {len(international)} international accounts to FATCA and "
        f"{len(domestic)} domestic accounts to IRS"
    )
    
    return international, domestic


@op(retry_policy=retry_policy)
def route_to_fatca(context: OpExecutionContext, international_accounts: List[Dict[str, Any]]) -> str:
    """Processes international accounts through FATCA regulatory reporting system."""
    if not international_accounts:
        context.log.info("No international accounts to process for FATCA")
        return ""
    
    fatca_resource = context.resources.fatca_system
    xml_report = fatca_resource.generate_xml_report(international_accounts)
    context.log.info(f"Generated FATCA XML report ({len(xml_report)} bytes)")
    return xml_report


@op(retry_policy=retry_policy)
def route_to_irs(context: OpExecutionContext, domestic_accounts: List[Dict[str, Any]]) -> str:
    """Processes domestic accounts through IRS regulatory reporting system."""
    if not domestic_accounts:
        context.log.info("No domestic accounts to process for IRS")
        return ""
    
    irs_resource = context.resources.irs_system
    form_1099_data = irs_resource.generate_form_1099(domestic_accounts)
    context.log.info(f"Generated IRS Form 1099 data ({len(form_1099_data)} bytes)")
    return form_1099_data


@op(
    retry_policy=retry_policy,
    config_schema=ArchiveConfig,
)
def archive_reports(
    context: OpExecutionContext,
    config: ArchiveConfig,
    fatca_reports: str,
    irs_reports: str,
) -> str:
    """Merges outputs from both reporting branches and archives them."""
    archive_resource = context.resources.archive_storage
    context.log.info(f"Archiving reports to {config.archive_location}")
    archive_result = archive_resource.archive_reports(
        fatca_reports, irs_reports, config.archive_location
    )
    context.log.info(f"Successfully archived reports: {archive_result}")
    return archive_result


@job(
    resource_defs={
        "csv_source": csv_file_system,
        "fatca_system": fatca_reporting_system,
        "irs_system": irs_reporting_system,
        "archive_storage": secure_archive_storage,
    },
    description="Regulatory report router pipeline with branch-merge pattern for financial compliance",
)
def regulatory_report_router():
    """Defines the pipeline with branch-merge pattern."""
    # Linear extraction
    transactions = read_csv()
    
    # Conditional branching
    international_accounts, domestic_accounts = account_check(transactions)
    
    # Parallel transformation branches
    fatca_reports = route_to_fatca(international_accounts)
    irs_reports = route_to_irs(domestic_accounts)
    
    # Synchronized merge
    archive_reports(fatca_reports, irs_reports)


# Daily schedule with no catch-up
daily_schedule = ScheduleDefinition(
    job=regulatory_report_router,
    cron_schedule="0 2 * * *",  # Daily at 2 AM UTC
    execution_timezone="UTC",
    catchup=False,
)


# Definitions object for Dagster deployment
defs = Definitions(
    jobs=[regulatory_report_router],
    schedules=[daily_schedule],
)


# Minimal launch pattern for local execution
if __name__ == "__main__":
    result = regulatory_report_router.execute_in_process(
        run_config={
            "ops": {
                "read_csv": {"config": {"csv_path": "financial_transactions.csv"}},
                "archive_reports": {"config": {"archive_location": "/secure/regulatory_archive"}},
            }
        }
    )
    print(f"Pipeline execution {'succeeded' if result.success else 'failed'}")