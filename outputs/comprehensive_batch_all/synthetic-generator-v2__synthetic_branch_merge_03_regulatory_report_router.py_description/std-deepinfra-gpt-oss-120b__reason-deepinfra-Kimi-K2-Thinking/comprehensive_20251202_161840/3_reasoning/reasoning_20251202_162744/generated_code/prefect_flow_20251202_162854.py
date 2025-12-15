from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
import pandas as pd
from typing import Tuple
import xml.etree.ElementTree as ET
from pathlib import Path
import zipfile

@task(retries=2, retry_delay_seconds=300)
def read_csv(csv_path: str) -> pd.DataFrame:
    """Read financial transaction CSV file."""
    path = Path(csv_path)
    if not path.exists():
        # Generate sample data for demonstration
        data = {
            'transaction_id': range(1, 6),
            'account_id': [f'ACC{i:03d}' for i in range(1, 6)],
            'account_type': ['domestic', 'international', 'domestic', 'international', 'domestic'],
            'amount': [1000.0, 2500.0, 1500.0, 3000.0, 2000.0],
            'currency': ['USD', 'EUR', 'USD', 'GBP', 'USD']
        }
        return pd.DataFrame(data)
    return pd.read_csv(csv_path)

@task(retries=2, retry_delay_seconds=300)
def account_check(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Analyze account types and split data for routing."""
    international_mask = df['account_type'].str.lower() == 'international'
    international_df = df[international_mask].copy()
    domestic_df = df[~international_mask].copy()
    return international_df, domestic_df

@task(retries=2, retry_delay_seconds=300)
def route_to_fatca(international_df: pd.DataFrame) -> str:
    """Process international accounts through FATCA reporting."""
    if international_df.empty:
        return ""
    
    root = ET.Element("FATCAReport")
    for _, row in international_df.iterrows():
        record = ET.SubElement(root, "Record")
        ET.SubElement(record, "TransactionID").text = str(row['transaction_id'])
        ET.SubElement(record, "AccountID").text = str(row['account_id'])
        ET.SubElement(record, "Amount").text = f"{row['amount']:.2f}"
        ET.SubElement(record, "Currency").text = str(row['currency'])
    
    output_path = "fatca_report.xml"
    ET.ElementTree(root).write(output_path, encoding='utf-8', xml_declaration=True)
    return output_path

@task(retries=2, retry_delay_seconds=300)
def route_to_irs(domestic_df: pd.DataFrame) -> str:
    """Process domestic accounts through IRS reporting."""
    if domestic_df.empty:
        return ""
    
    output_path = "irs_form_1099.csv"
    domestic_df.to_csv(output_path, index=False)
    return output_path

@task(retries=2, retry_delay_seconds=300)
def archive_reports(fatca_path: str, irs_path: str) -> str:
    """Merge and compress reports for archival."""
    archive_name = "regulatory_reports_archive.zip"
    
    with zipfile.ZipFile(archive_name, 'w', zipfile.ZIP_DEFLATED) as zf:
        if fatca_path and Path(fatca_path).exists():
            zf.write(fatca_path)
        if irs_path and Path(irs_path).exists():
            zf.write(irs_path)
    
    return str(Path(archive_name).absolute())

@flow(
    name="regulatory-report-router",
    task_runner=ConcurrentTaskRunner(),
)
def regulatory_report_router(csv_path: str = "transactions.csv"):
    """
    Regulatory report router pipeline.
    
    Daily execution schedule with no catch-up.
    Retry: 2 attempts with 5-minute delay between retries.
    """
    # Extract phase
    df = read_csv(csv_path)
    
    # Routing decision phase
    international_df, domestic_df = account_check(df)
    
    # Parallel transformation phase
    fatca_future = route_to_fatca.submit(international_df)
    irs_future = route_to_irs.submit(domestic_df)
    
    # Merge and archive phase
    archive_reports(fatca_future, irs_future)

if __name__ == '__main__':
    regulatory_report_router()