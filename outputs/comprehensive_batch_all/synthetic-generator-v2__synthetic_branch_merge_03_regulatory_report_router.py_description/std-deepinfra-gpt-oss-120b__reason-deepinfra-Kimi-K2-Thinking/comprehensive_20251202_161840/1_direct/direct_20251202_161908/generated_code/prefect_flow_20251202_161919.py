from pathlib import Path
import zipfile
from datetime import datetime
from typing import List, Tuple

import pandas as pd
from prefect import flow, task
from prefect.logging import get_logger


@task(retries=2, retry_delay_seconds=300)
def read_csv(file_path: str) -> pd.DataFrame:
    """Read the financial transaction CSV file."""
    logger = get_logger()
    logger.info("Reading CSV file from %s", file_path)
    df = pd.read_csv(file_path)
    logger.info("Loaded %d rows", len(df))
    return df


@task(retries=2, retry_delay_seconds=300)
def account_check(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split transactions into international and domestic accounts.

    Returns a tuple (international_df, domestic_df).
    """
    logger = get_logger()
    if "account_type" not in df.columns:
        raise ValueError("Column 'account_type' not found in input data.")
    international_df = df[df["account_type"] == "international"].copy()
    domestic_df = df[df["account_type"] == "domestic"].copy()
    logger.info(
        "Account check: %d international, %d domestic",
        len(international_df),
        len(domestic_df),
    )
    return international_df, domestic_df


@task(retries=2, retry_delay_seconds=300)
def route_to_fatca(international_df: pd.DataFrame) -> Path:
    """
    Process international accounts for FATCA reporting.

    Generates a simple XML report and returns the file path.
    """
    logger = get_logger()
    output_dir = Path("reports/fatca")
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    report_path = output_dir / f"fatca_report_{timestamp}.xml"

    logger.info("Generating FATCA XML report at %s", report_path)
    # Minimal XML generation
    xml_content = ["<?xml version='1.0' encoding='UTF-8'?>", "<FATCAReport>"]
    for _, row in international_df.iterrows():
        xml_content.append(
            f"  <Transaction id='{row.get('transaction_id', '')}'>"
            f"<Amount>{row.get('amount', '')}</Amount>"
            f"<Account>{row.get('account_id', '')}</Account>"
            "</Transaction>"
        )
    xml_content.append("</FATCAReport>")
    report_path.write_text("\n".join(xml_content), encoding="utf-8")
    logger.info("FATCA report generated with %d transactions", len(international_df))
    return report_path


@task(retries=2, retry_delay_seconds=300)
def route_to_irs(domestic_df: pd.DataFrame) -> Path:
    """
    Process domestic accounts for IRS reporting.

    Generates a CSV report (simulating Form 1099) and returns the file path.
    """
    logger = get_logger()
    output_dir = Path("reports/irs")
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    report_path = output_dir / f"irs_report_{timestamp}.csv"

    logger.info("Generating IRS CSV report at %s", report_path)
    # For simplicity, write the dataframe directly
    domestic_df.to_csv(report_path, index=False)
    logger.info("IRS report generated with %d transactions", len(domestic_df))
    return report_path


@task(retries=2, retry_delay_seconds=300)
def archive_reports(report_paths: List[Path]) -> Path:
    """
    Merge and compress all regulatory reports into a secure archive.

    Returns the path to the created zip archive.
    """
    logger = get_logger()
    archive_dir = Path("archive")
    archive_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    archive_path = archive_dir / f"regulatory_reports_{timestamp}.zip"

    logger.info("Creating archive %s", archive_path)
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
        for report_path in report_paths:
            zipf.write(report_path, arcname=report_path.name)
            logger.debug("Added %s to archive", report_path)
    logger.info("Archive created with %d files", len(report_paths))
    return archive_path


@flow
def regulatory_report_router(csv_path: str = "transactions.csv") -> Path:
    """
    Orchestrates the regulatory report routing pipeline.

    Execution order:
    1. Read CSV
    2. Account check (branching)
    3. Parallel FATCA and IRS processing
    4. Merge and archive reports
    """
    logger = get_logger()
    logger.info("Starting regulatory report router flow")

    df = read_csv(csv_path)
    international_df, domestic_df = account_check(df)

    # Run reporting tasks in parallel
    fatca_future = route_to_fatca.submit(international_df)
    irs_future = route_to_irs.submit(domestic_df)

    # Wait for both to complete
    fatca_report = fatca_future.result()
    irs_report = irs_future.result()

    archive_path = archive_reports([fatca_report, irs_report])
    logger.info("Flow completed. Archive stored at %s", archive_path)
    return archive_path


# Deployment note:
# This flow is intended to run daily with no catch‑up.
# Schedule configuration should be applied in the Prefect deployment YAML or UI.

if __name__ == "__main__":
    # Local execution for testing
    regulatory_report_router()