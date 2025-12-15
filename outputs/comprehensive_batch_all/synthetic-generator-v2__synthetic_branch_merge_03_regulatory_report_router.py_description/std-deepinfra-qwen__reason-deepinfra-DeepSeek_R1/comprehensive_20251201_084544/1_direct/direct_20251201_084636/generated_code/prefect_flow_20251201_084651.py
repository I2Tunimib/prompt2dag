from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
import pandas as pd

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def read_csv(file_path: str) -> pd.DataFrame:
    """Reads financial transaction CSV file."""
    return pd.read_csv(file_path)

@task
def account_check(df: pd.DataFrame) -> dict:
    """Analyzes account types and determines routing path."""
    international_accounts = df[df['account_type'] == 'international']
    domestic_accounts = df[df['account_type'] == 'domestic']
    return {'international': international_accounts, 'domestic': domestic_accounts}

@task
def route_to_fatca(international_accounts: pd.DataFrame) -> str:
    """Processes international accounts through FATCA regulatory reporting system."""
    # Simulate FATCA report generation
    fatca_report = "FATCA_report.xml"
    # Save or process the report
    return fatca_report

@task
def route_to_irs(domestic_accounts: pd.DataFrame) -> str:
    """Processes domestic accounts through IRS regulatory reporting system."""
    # Simulate IRS report generation
    irs_report = "IRS_report_1099.csv"
    # Save or process the report
    return irs_report

@task
def archive_reports(fatca_report: str, irs_report: str) -> None:
    """Merges outputs from both reporting branches, compresses all regulatory reports, and stores them in secure archive location."""
    # Simulate report archiving
    logger = get_run_logger()
    logger.info(f"Archiving reports: {fatca_report}, {irs_report}")
    # Archive logic here

@flow(retries=2, retry_delay_seconds=300)
def regulatory_report_router(file_path: str):
    """Orchestrates the regulatory report routing pipeline."""
    df = read_csv(file_path)
    account_types = account_check(df)
    
    fatca_report = route_to_fatca.submit(account_types['international'])
    irs_report = route_to_irs.submit(account_types['domestic'])
    
    archive_reports(fatca_report, irs_report)

if __name__ == '__main__':
    # Example file path
    file_path = "transactions.csv"
    regulatory_report_router(file_path)

# Deployment/schedule configuration (optional)
# Schedule: Daily execution, no catch-up, 2 retries with 5-minute delay
# Deployment: Prefect deployment with appropriate infrastructure and environment variables