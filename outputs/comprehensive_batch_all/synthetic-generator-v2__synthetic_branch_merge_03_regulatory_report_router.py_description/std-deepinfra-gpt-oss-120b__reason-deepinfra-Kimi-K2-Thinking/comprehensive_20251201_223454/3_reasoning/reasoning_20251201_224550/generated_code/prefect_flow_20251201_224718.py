from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
import pandas as pd
from typing import Tuple, Optional
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Mock external system integrations
def _mock_fatca_system(data: pd.DataFrame) -> str:
    """Simulate FATCA regulatory reporting system."""
    timestamp = int(time.time())
    report_path = f"/reports/fatca_report_{timestamp}.xml"
    logger.info(f"FATCA system generated: {report_path}")
    return report_path


def _mock_irs_system(data: pd.DataFrame) -> str:
    """Simulate IRS regulatory reporting system."""
    timestamp = int(time.time())
    report_path = f"/reports/irs_report_{timestamp}.pdf"
    logger.info(f"IRS system generated: {report_path}")
    return report_path


def _mock_secure_archive(reports: list) -> str:
    """Simulate secure archive storage system."""
    timestamp = int(time.time())
    archive_path = f"/secure/archive/regulatory_bundle_{timestamp}.zip"
    logger.info(f"Secure archive created: {archive_path}")
    return archive_path


@task(
    name="read-financial-csv",
    retries=2,
    retry_delay_seconds=300,
    description="Reads financial transaction CSV file with account type information"
)
def read_financial_csv(csv_path: str) -> pd.DataFrame:
    """Read and load financial transaction data from CSV source."""
    try:
        logger.info(f"Reading financial transactions from: {csv_path}")
        
        # Mock data for demonstration; replace with pd.read_csv(csv_path) in production
        data = {
            "transaction_id": range(1, 11),
            "account_type": [
                "international", "domestic", "international", "domestic",
                "international", "domestic", "international", "domestic",
                "international", "domestic"
            ],
            "amount": [1000.0, 2000.0, 3000.0, 4000.0, 5000.0,
                      6000.0, 7000.0, 8000.0, 9000.0, 10000.0],
            "account_id": [f"ACC_{i:03d}" for i in range(1, 11)]
        }
        df = pd.DataFrame(data)
        logger.info(f"Successfully loaded {len(df)} transactions")
        return df
    except Exception as e:
        logger.error(f"CSV read failed: {e}")
        raise


@task(
    name="analyze-account-types",
    retries=2,
    retry_delay_seconds=300,
    description="Analyzes account types and determines routing paths"
)
def analyze_account_types(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Split transactions into international and domestic datasets for routing."""
    try:
        logger.info("Analyzing account types for regulatory routing")
        
        international_mask = df["account_type"].str.lower() == "international"
        international_data = df[international_mask].copy()
        domestic_data = df[~international_mask].copy()
        
        logger.info(f"International accounts: {len(international_data)}")
        logger.info(f"Domestic accounts: {len(domestic_data)}")
        
        return international_data, domestic_data
    except Exception as e:
        logger.error(f"Account type analysis failed: {e}")
        raise


@task(
    name="process-fatca-reports",
    retries=2,
    retry_delay_seconds=300,
    description="Processes international accounts through FATCA reporting system"
)
def process_fatca_reports(international_data: pd.DataFrame) -> Optional[str]:
    """Generate FATCA XML reports for international accounts."""
    try:
        if international_data.empty:
            logger.info("No international accounts to process for FATCA")
            return None
        
        logger.info(f"Processing {len(international_data)} accounts through FATCA")
        report_path = _mock_fatca_system(international_data)
        return report_path
    except Exception as e:
        logger.error(f"FATCA processing failed: {e}")
        raise


@task(
    name="process-irs-reports",
    retries=2,
    retry_delay_seconds=300,
    description="Processes domestic accounts through IRS reporting system"
)
def process_irs_reports(domestic_data: pd.DataFrame) -> Optional[str]:
    """Generate IRS Form 1099 reports for domestic accounts."""
    try:
        if domestic_data.empty:
            logger.info("No domestic accounts to process for IRS")
            return None
        
        logger.info(f"Processing {len(domestic_data)} accounts through IRS")
        report_path = _mock_irs_system(domestic_data)
        return report_path
    except Exception as e:
        logger.error(f"IRS processing failed: {e}")
        raise


@task(
    name="archive-regulatory-reports",
    retries=2,
    retry_delay_seconds=300,
    description="Merges and archives all regulatory reports"
)
def archive_regulatory_reports(
    fatca_report: Optional[str],
    irs_report: Optional[str]
) -> str:
    """Compress and store generated reports in secure archive location."""
    try:
        reports = [r for r in [fatca_report, irs_report] if r is not None]
        
        if not reports:
            logger.warning("No reports available for archival")
            return "No reports to archive"
        
        logger.info(f"Archiving {len(reports)} regulatory report(s)")
        archive_path = _mock_secure_archive(reports)
        return archive_path
    except Exception as e:
        logger.error(f"Report archival failed: {e}")
        raise


@flow(
    name="regulatory-report-router",
    description="Financial transaction regulatory report router with branch-merge pattern",
    task_runner=ConcurrentTaskRunner(),
    # Deployment schedule configuration:
    # schedule=prefect.schedules.Cron("0 2 * * *"),  # Daily at 2 AM
    # catchup=False,
)
def regulatory_report_router(csv_path: str = "/data/transactions.csv") -> str:
    """
    Orchestrates regulatory report routing pipeline.
    
    Execution order:
    1. Linear extraction (read CSV)
    2. Conditional branching (account type analysis)
    3. Parallel transformation (FATCA and IRS processing)
    4. Synchronized merge (archive reports)
    """
    # Step 1: Extract transaction data
    transaction_data = read_financial_csv(csv_path)
    
    # Step 2: Branch - analyze and split by account type
    international_data, domestic_data = analyze_account_types(transaction_data)
    
    # Step 3: Parallel transform - process both regulatory paths concurrently
    fatca_future = process_fatca_reports.submit(international_data)
    irs_future = process_irs_reports.submit(domestic_data)
    
    # Step 4: Merge - archive results from both branches
    archive_location = archive_regulatory_reports(fatca_future, irs_future)
    
    logger.info(f"Pipeline completed successfully. Archive: {archive_location}")
    return archive_location


if __name__ == "__main__":
    logger.info("Starting regulatory report router pipeline (local execution)")
    result = regulatory_report_router()
    print(f"Flow execution complete. Archive location: {result}")