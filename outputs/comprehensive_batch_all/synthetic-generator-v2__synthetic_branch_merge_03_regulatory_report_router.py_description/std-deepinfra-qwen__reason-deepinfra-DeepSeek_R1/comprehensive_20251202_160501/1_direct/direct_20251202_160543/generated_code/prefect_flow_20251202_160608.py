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
    fatca_report = "FATCA_Report.xml"
    return fatca_report

@task
def route_to_irs(domestic_accounts: pd.DataFrame) -> str:
    """Processes domestic accounts through IRS regulatory reporting system."""
    # Simulate IRS report generation
    irs_report = "IRS_Report_1099.csv"
    return irs_report

@task
def archive_reports(fatca_report: str, irs_report: str) -> None:
    """Merges outputs from both reporting branches, compresses all regulatory reports, and stores them in a secure archive location."""
    import zipfile
    with zipfile.ZipFile('regulatory_reports.zip', 'w') as zipf:
        zipf.write(fatca_report)
        zipf.write(irs_report)
    logger = get_run_logger()
    logger.info("Reports archived successfully.")

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
    file_path = 'transactions.csv'
    regulatory_report_router(file_path)