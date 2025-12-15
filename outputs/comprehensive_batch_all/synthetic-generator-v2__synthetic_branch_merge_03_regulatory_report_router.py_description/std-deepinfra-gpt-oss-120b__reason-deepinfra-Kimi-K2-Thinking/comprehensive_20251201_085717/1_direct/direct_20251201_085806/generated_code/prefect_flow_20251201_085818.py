from pathlib import Path
import zipfile
from datetime import datetime
from typing import List, Tuple

import pandas as pd
from prefect import flow, task
from prefect.tasks import task_input_hash


@task(
    retries=2,
    retry_delay_seconds=300,
    cache_key_fn=task_input_hash,
    cache_expiration=None,
)
def read_csv(csv_path: Path) -> pd.DataFrame:
    """
    Read the financial transaction CSV file.

    Args:
        csv_path: Path to the CSV file.

    Returns:
        DataFrame containing the transaction data.
    """
    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    df = pd.read_csv(csv_path)
    return df


@task(
    retries=2,
    retry_delay_seconds=300,
)
def account_check(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split transactions based on account type.

    Args:
        df: DataFrame with transaction data. Expected column 'account_type'.

    Returns:
        Tuple containing (international_df, domestic_df).
    """
    if "account_type" not in df.columns:
        raise ValueError("Column 'account_type' missing from input data.")
    international_df = df[df["account_type"] == "international"].copy()
    domestic_df = df[df["account_type"] == "domestic"].copy()
    return international_df, domestic_df


@task(
    retries=2,
    retry_delay_seconds=300,
)
def route_to_fatca(international_df: pd.DataFrame, output_dir: Path) -> List[Path]:
    """
    Process international accounts for FATCA reporting.

    Generates simple XML files for each transaction.

    Args:
        international_df: DataFrame with international account transactions.
        output_dir: Directory where FATCA reports will be stored.

    Returns:
        List of paths to generated XML report files.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    report_paths = []
    for idx, row in international_df.iterrows():
        report_name = f"fatca_report_{idx}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.xml"
        report_path = output_dir / report_name
        xml_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<FATCAReport>
    <TransactionID>{row.get('transaction_id', idx)}</TransactionID>
    <Amount>{row.get('amount', '')}</Amount>
    <AccountNumber>{row.get('account_number', '')}</AccountNumber>
</FATCAReport>
"""
        report_path.write_text(xml_content, encoding="utf-8")
        report_paths.append(report_path)
    return report_paths


@task(
    retries=2,
    retry_delay_seconds=300,
)
def route_to_irs(domestic_df: pd.DataFrame, output_dir: Path) -> List[Path]:
    """
    Process domestic accounts for IRS reporting.

    Generates simple JSON files representing Form 1099 data.

    Args:
        domestic_df: DataFrame with domestic account transactions.
        output_dir: Directory where IRS reports will be stored.

    Returns:
        List of paths to generated JSON report files.
    """
    import json

    output_dir.mkdir(parents=True, exist_ok=True)
    report_paths = []
    for idx, row in domestic_df.iterrows():
        report_name = f"irs_report_{idx}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.json"
        report_path = output_dir / report_name
        json_content = {
            "TransactionID": row.get("transaction_id", idx),
            "Amount": row.get("amount", ""),
            "AccountNumber": row.get("account_number", ""),
            "Form": "1099",
        }
        report_path.write_text(json.dumps(json_content, indent=2), encoding="utf-8")
        report_paths.append(report_path)
    return report_paths


@task(
    retries=2,
    retry_delay_seconds=300,
)
def archive_reports(
    fatca_reports: List[Path],
    irs_reports: List[Path],
    archive_dir: Path,
) -> Path:
    """
    Merge and archive all regulatory reports into a zip file.

    Args:
        fatca_reports: List of FATCA report file paths.
        irs_reports: List of IRS report file paths.
        archive_dir: Directory where the archive will be stored.

    Returns:
        Path to the created zip archive.
    """
    archive_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    archive_path = archive_dir / f"regulatory_reports_{timestamp}.zip"

    all_reports = fatca_reports + irs_reports
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
        for report_path in all_reports:
            if report_path.is_file():
                zipf.write(report_path, arcname=report_path.name)
    return archive_path


@flow
def regulatory_report_router(
    csv_path: str = "data/transactions.csv",
    fatca_output_dir: str = "output/fatca",
    irs_output_dir: str = "output/irs",
    archive_output_dir: str = "output/archive",
) -> Path:
    """
    Orchestrates the regulatory report routing pipeline.

    Execution order:
        1. Read CSV
        2. Account type check
        3. Parallel processing for FATCA and IRS reports
        4. Archive merged reports

    Returns:
        Path to the final archive file.
    """
    csv_path_obj = Path(csv_path)
    fatca_dir = Path(fatca_output_dir)
    irs_dir = Path(irs_output_dir)
    archive_dir = Path(archive_output_dir)

    df = read_csv(csv_path_obj)
    international_df, domestic_df = account_check(df)

    # Run reporting tasks in parallel
    fatca_future = route_to_fatca.submit(international_df, fatca_dir)
    irs_future = route_to_irs.submit(domestic_df, irs_dir)

    # Wait for both to complete and collect results
    fatca_reports = fatca_future.result()
    irs_reports = irs_future.result()

    archive_path = archive_reports(fatca_reports, irs_reports, archive_dir)
    return archive_path


if __name__ == "__main__":
    # Note: In production, configure a Prefect deployment with a daily schedule,
    # no catch-up, and the retry settings defined on each task.
    final_archive = regulatory_report_router()
    print(f"Archive created at: {final_archive}")