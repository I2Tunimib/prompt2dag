import pandas as pd
from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from typing import Tuple
import tempfile
import os
from pathlib import Path
import time


@task(retries=2, retry_delay_seconds=300)
def read_csv(csv_path: str) -> pd.DataFrame:
    """Read financial transaction CSV file."""
    if not os.path.exists(csv_path):
        sample_data = {
            'account_id': [1, 2, 3, 4, 5],
            'account_type': ['international', 'domestic', 'international', 'domestic', 'domestic'],
            'amount': [1000.0, 2000.0, 1500.0, 3000.0, 2500.0],
            'currency': ['USD', 'USD', 'EUR', 'USD', 'USD']
        }
        df = pd.DataFrame(sample_data)
        df.to_csv(csv_path, index=False)
    
    return pd.read_csv(csv_path)


@task(retries=2, retry_delay_seconds=300)
def account_check(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Analyze account types and split data for routing."""
    international_df = df[df['account_type'] == 'international'].copy()
    domestic_df = df[df['account_type'] == 'domestic'].copy()
    return international_df, domestic_df


@task(retries=2, retry_delay_seconds=300)
def route_to_fatca(international_df: pd.DataFrame) -> str:
    """Process international accounts through FATCA reporting."""
    output_dir = Path(tempfile.gettempdir()) / "fatca_reports"
    output_dir.mkdir(exist_ok=True)
    
    report_path = output_dir / f"fatca_report_{int(time.time())}.xml"
    
    xml_content = f"""<?xml version="1.0"?>
<FATCAReport>
    <RecordCount>{len(international_df)}</RecordCount>
    <TotalAmount>{international_df['amount'].sum():.2f}</TotalAmount>
</FATCAReport>"""
    
    report_path.write_text(xml_content)
    return str(report_path)


@task(retries=2, retry_delay_seconds=300)
def route_to_irs(domestic_df: pd.DataFrame) -> str:
    """Process domestic accounts through IRS reporting."""
    output_dir = Path(tempfile.gettempdir()) / "irs_reports"
    output_dir.mkdir(exist_ok=True)
    
    report_path = output_dir / f"irs_report_{int(time.time())}.csv"
    domestic_df.to_csv(report_path, index=False)
    return str(report_path)


@task(retries=2, retry_delay_seconds=300)
def archive_reports(fatca_report_path: str, irs_report_path: str) -> str:
    """Merge and archive reports from both branches."""
    archive_dir = Path(tempfile.gettempdir()) / "archive"
    archive_dir.mkdir(exist_ok=True)
    
    archive_path = archive_dir / f"archive_{int(time.time())}.txt"
    archive_content = f"FATCA Report: {fatca_report_path}\nIRS Report: {irs_report_path}"
    archive_path.write_text(archive_content)
    
    return str(archive_path)


@flow(
    name="regulatory-report-router",
    task_runner=ConcurrentTaskRunner(),
)
def regulatory_report_router(csv_path: str = "transactions.csv") -> str:
    """
    Process financial transactions and route to appropriate regulatory reporting systems.
    
    Execution flow:
    1. Extract: Read CSV data
    2. Branch: Split by account type
    3. Transform (parallel): FATCA and IRS processing
    4. Merge: Archive both reports
    
    Deployment: Configure with daily schedule (no catchup) for production.
    """
    df = read_csv(csv_path)
    
    international_df, domestic_df = account_check(df)
    
    fatca_future = route_to_fatca.submit(international_df)
    irs_future = route_to_irs.submit(domestic_df)
    
    archive_path = archive_reports(fatca_future, irs_future)
    
    return archive_path


if __name__ == "__main__":
    regulatory_report_router()