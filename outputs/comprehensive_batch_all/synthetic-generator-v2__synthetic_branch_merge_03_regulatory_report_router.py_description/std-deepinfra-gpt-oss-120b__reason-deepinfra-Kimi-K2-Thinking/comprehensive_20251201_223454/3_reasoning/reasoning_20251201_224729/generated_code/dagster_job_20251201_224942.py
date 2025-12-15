from dagster import (
    op,
    job,
    Out,
    Output,
    RetryPolicy,
    ScheduleDefinition,
    Definitions,
    Config,
    RunConfig,
    OpExecutionContext,
)
import csv
import os
from typing import List, Dict, Any
from datetime import datetime


# Type aliases
Transaction = Dict[str, Any]
Transactions = List[Transaction]


# Config classes
class CsvConfig(Config):
    file_path: str = "financial_transactions.csv"


class FatcaConfig(Config):
    endpoint: str = "https://fatca-reporting.example.com/api"
    timeout: int = 30


class IrsConfig(Config):
    endpoint: str = "https://irs-reporting.example.com/api"
    timeout: int = 30


class ArchiveConfig(Config):
    storage_path: str = "/secure/archive/regulatory_reports"


# Resource stubs for external systems
class FatcaReportingResource:
    """Stub for FATCA regulatory reporting system."""
    
    def __init__(self, endpoint: str, timeout: int):
        self.endpoint = endpoint
        self.timeout = timeout
    
    def generate_xml_report(self, transactions: Transactions) -> str:
        return f"<fatca_report><transaction_count>{len(transactions)}</transaction_count></fatca_report>"


class IrsReportingResource:
    """Stub for IRS regulatory reporting system."""
    
    def __init__(self, endpoint: str, timeout: int):
        self.endpoint = endpoint
        self.timeout = timeout
    
    def generate_form_1099_data(self, transactions: Transactions) -> str:
        return f"form_1099_data: {len(transactions)} transactions processed"


class ArchiveStorageResource:
    """Stub for secure archive storage system."""
    
    def __init__(self, storage_path: str):
        self.storage_path = storage_path
    
    def store_reports(self, fatca_report: str, irs_report: str) -> str:
        timestamp = datetime.now().isoformat()
        os.makedirs(self.storage_path, exist_ok=True)
        archive_path = f"{self.storage_path}/reports_{timestamp}.zip"
        return archive_path


def _create_sample_csv(file_path: str):
    """Creates a sample CSV file for testing purposes."""
    sample_data = [
        {"transaction_id": "1", "account_type": "international", "amount": "1000.00", "currency": "USD"},
        {"transaction_id": "2", "account_type": "domestic", "amount": "500.00", "currency": "USD"},
        {"transaction_id": "3", "account_type": "international", "amount": "2000.00", "currency": "EUR"},
        {"transaction_id": "4", "account_type": "domestic", "amount": "750.00", "currency": "USD"},
        {"transaction_id": "5", "account_type": "unknown", "amount": "300.00", "currency": "USD"},
    ]
    
    with open(file_path, 'w', newline='') as csvfile:
        fieldnames = ["transaction_id", "account_type", "amount", "currency"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(sample_data)


@op(
    description="Reads financial transaction CSV file containing transaction data with account type information",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def read_csv(context: OpExecutionContext, config: CsvConfig) -> Transactions:
    """Reads a CSV file and returns a list of transaction dictionaries."""
    context.log.info(f"Reading CSV file from {config.file_path}")
    
    if not os.path.exists(config.file_path):
        context.log.warning(f"CSV file not found, creating sample data at {config.file_path}")
        _create_sample_csv(config.file_path)
    
    transactions = []
    try:
        with open(config.file_path, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                transactions.append(dict(row))
        
        context.log.info(f"Successfully read {len(transactions)} transactions")
        return transactions
    except Exception as e:
        context.log.error(f"Error reading CSV file: {e}")
        raise


@op(
    out={
        "international_accounts": Out(description="Transactions for international accounts"),
        "domestic_accounts": Out(description="Transactions for domestic accounts"),
    },
    description="Analyzes account types and determines routing path using conditional branching logic",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def account_check(context: OpExecutionContext, transactions: Transactions):
    """Splits transactions into international and domestic based on account type."""
    context.log.info(f"Analyzing {len(transactions)} transactions for account types")
    
    international = []
    domestic = []
    
    for transaction in transactions:
        account_type = transaction.get('account_type', 'unknown').lower()
        
        if account_type in ['international', 'foreign', 'offshore']:
            international.append(transaction)
        else:
            if account_type not in ['domestic', 'local', 'us']:
                context.log.warning(f"Unknown account type '{account_type}', defaulting to domestic")
            domestic.append(transaction)
    
    context.log.info(f"Found {len(international)} international and {len(domestic)} domestic accounts")
    
    yield Output(international, output_name="international_accounts")
    yield Output(domestic, output_name="domestic_accounts")


@op(
    description="Processes international accounts through FATCA regulatory reporting system, generating XML reports",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def route_to_fatca(
    context: OpExecutionContext, 
    international_accounts: Transactions,
    config: FatcaConfig,
) -> str:
    """Processes international accounts through FATCA reporting."""
    context.log.info(f"Processing {len(international_accounts)} international accounts through FATCA")
    
    if not international_accounts:
        context.log.info("No international accounts to process")
        return "<fatca_report><no_transactions/></fatca_report>"
    
    fatca_resource = FatcaReportingResource(config.endpoint, config.timeout)
    
    try:
        report = fatca_resource.generate_xml_report(international_accounts)
        context.log.info("FATCA XML report generated successfully")
        return report
    except Exception as e:
        context.log.error(f"Error generating FATCA report: {e}")
        raise


@op(
    description="Processes domestic accounts through IRS regulatory reporting system, generating Form 1099 data",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def route_to_irs(
    context: OpExecutionContext, 
    domestic_accounts: Transactions,
    config: IrsConfig,
) -> str:
    """Processes domestic accounts through IRS reporting."""
    context.log.info(f"Processing {len(domestic_accounts)} domestic accounts through IRS")
    
    if not domestic_accounts:
        context.log.info("No domestic accounts to process")
        return "form_1099_data: no transactions"
    
    irs_resource = IrsReportingResource(config.endpoint, config.timeout)
    
    try:
        report = irs_resource.generate_form_1099_data(domestic_accounts)
        context.log.info("IRS Form 1099 data generated successfully")
        return report
    except Exception as e:
        context.log.error(f"Error generating IRS report: {e}")
        raise


@op(
    description="Merges outputs from both reporting branches, compresses all regulatory reports, and stores them in secure archive location",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def archive_reports(
    context: OpExecutionContext,
    fatca_report: str,
    irs_report: str,
    config: ArchiveConfig,
) -> str:
    """Merges and archives reports from both FATCA and IRS branches."""
    context.log.info("Merging and archiving regulatory reports")
    
    archive_resource = ArchiveStorageResource(config.storage_path)
    
    try:
        archive_path = archive_resource.store_reports(fatca_report, irs_report)
        context.log.info(f"Reports successfully archived to {archive_path}")
        return archive_path
    except Exception as e:
        context.log.error(f"Error archiving reports: {e}")
        raise


@job(
    description="Regulatory report router pipeline for financial transaction data",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
)
def regulatory_report_router():
    """Defines the regulatory report router pipeline with branch-merge pattern.
    
    Execution flow:
    1. Linear extraction: read_csv
    2. Conditional branching: account_check splits data
    3. Parallel transformation: route_to_fatca and route_to_irs run concurrently
    4. Synchronized merge: archive_reports waits for both branches
    """
    transactions = read_csv()
    international_accounts, domestic_accounts = account_check(transactions)
    fatca_report = route_to_fatca(international_accounts)
    irs_report = route_to_irs(domestic_accounts)
    archive_reports(fatca_report, irs_report)


# Daily schedule at 2 AM UTC with no catch-up
daily_schedule = ScheduleDefinition(
    job=regulatory_report_router,
    cron_schedule="0 2 * * *",
    execution_timezone="UTC",
    catchup=False,
)


# Definitions object to tie everything together
defs = Definitions(
    jobs=[regulatory_report_router],
    schedules=[daily_schedule],
)


# Minimal launch pattern for local execution
if __name__ == "__main__":
    # Create sample data if needed
    if not os.path.exists("financial_transactions.csv"):
        _create_sample_csv("financial_transactions.csv")
    
    # Execute pipeline
    result = regulatory_report_router.execute_in_process(
        run_config=RunConfig(
            ops={
                "read_csv": CsvConfig(),
                "route_to_fatca": FatcaConfig(),
                "route_to_irs": IrsConfig(),
                "archive_reports": ArchiveConfig(),
            }
        )
    )
    
    if result.success:
        print("✅ Pipeline execution completed successfully!")
    else:
        print("❌ Pipeline execution failed!")
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                print(f"Step failed: {event.step_key}")