from datetime import timedelta
import csv
import os
import zipfile
from typing import List, Dict

from dagster import (
    op,
    job,
    In,
    Out,
    Output,
    RetryPolicy,
    Config,
    ConfigurableResource,
    ScheduleDefinition,
    DefaultScheduleStatus,
    Definitions,
)


class CSVResource(ConfigurableResource):
    """Simple resource to provide CSV file path."""

    csv_path: str

    def read_transactions(self) -> List[Dict[str, str]]:
        """Read CSV file and return list of transaction dicts."""
        if not os.path.isfile(self.csv_path):
            raise FileNotFoundError(f"CSV file not found: {self.csv_path}")

        with open(self.csv_path, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            return [row for row in reader]


class ArchiveResource(ConfigurableResource):
    """Resource that defines where to store the archived reports."""

    archive_path: str

    def ensure_dir(self) -> None:
        os.makedirs(self.archive_path, exist_ok=True)


@op(
    required_resource_keys={"csv"},
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    out={"transactions": Out(List[Dict[str, str]])},
)
def read_csv(context) -> List[Dict[str, str]]:
    """Read financial transaction CSV file."""
    csv_resource: CSVResource = context.resources.csv
    transactions = csv_resource.read_transactions()
    context.log.info(f"Read {len(transactions)} transactions from CSV.")
    return transactions


@op(
    out={"international": Out(List[Dict[str, str]]), "domestic": Out(List[Dict[str, str]])},
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
)
def account_check(context, transactions: List[Dict[str, str]]) -> None:
    """
    Separate transactions into international and domestic based on the 'account_type' field.
    Expected values: 'international' or 'domestic'.
    """
    international = []
    domestic = []
    for tx in transactions:
        acct_type = tx.get("account_type", "").strip().lower()
        if acct_type == "international":
            international.append(tx)
        elif acct_type == "domestic":
            domestic.append(tx)
        else:
            context.log.warning(f"Unknown account_type '{acct_type}' in transaction {tx}")

    context.log.info(
        f"Separated {len(international)} international and {len(domestic)} domestic transactions."
    )
    yield Output(international, "international")
    yield Output(domestic, "domestic")


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    out=Out(List[str]),
)
def route_to_fatca(context, international: List[Dict[str, str]]) -> List[str]:
    """
    Process international accounts through FATCA reporting.
    Generates dummy XML report files and returns their paths.
    """
    report_paths = []
    for idx, tx in enumerate(international, start=1):
        filename = f"fatca_report_{idx}.xml"
        content = f"<FATCAReport><TransactionID>{tx.get('transaction_id')}</TransactionID></FATCAReport>"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(content)
        report_paths.append(os.path.abspath(filename))
        context.log.info(f"Generated FATCA report: {filename}")

    return report_paths


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    out=Out(List[str]),
)
def route_to_irs(context, domestic: List[Dict[str, str]]) -> List[str]:
    """
    Process domestic accounts through IRS reporting.
    Generates dummy 1099 report files and returns their paths.
    """
    report_paths = []
    for idx, tx in enumerate(domestic, start=1):
        filename = f"irs_1099_report_{idx}.txt"
        content = f"IRS 1099 Report - Transaction ID: {tx.get('transaction_id')}"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(content)
        report_paths.append(os.path.abspath(filename))
        context.log.info(f"Generated IRS report: {filename}")

    return report_paths


@op(
    required_resource_keys={"archive"},
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
)
def archive_reports(
    context,
    fatca_reports: List[str],
    irs_reports: List[str],
) -> str:
    """
    Merge all generated reports, compress them into a zip archive,
    and store the archive in a secure location.
    Returns the path to the created archive file.
    """
    archive_resource: ArchiveResource = context.resources.archive
    archive_resource.ensure_dir()
    archive_file = os.path.join(archive_resource.archive_path, "regulatory_reports.zip")

    with zipfile.ZipFile(archive_file, "w", compression=zipfile.ZIP_DEFLATED) as zipf:
        for report_path in fatca_reports + irs_reports:
            if os.path.isfile(report_path):
                zipf.write(report_path, arcname=os.path.basename(report_path))
                context.log.info(f"Added {report_path} to archive.")
            else:
                context.log.warning(f"Report file not found and skipped: {report_path}")

    context.log.info(f"Created archive at {archive_file}")
    return archive_file


@job(
    resource_defs={
        "csv": CSVResource,
        "archive": ArchiveResource,
    }
)
def regulatory_report_router():
    """Dagster job that routes financial transactions to appropriate regulatory reports."""
    transactions = read_csv()
    split = account_check(transactions)
    fatca_paths = route_to_fatca(split["international"])
    irs_paths = route_to_irs(split["domestic"])
    archive_reports(fatca_paths, irs_paths)


daily_schedule = ScheduleDefinition(
    job=regulatory_report_router,
    cron_schedule="0 0 * * *",  # Every day at midnight UTC
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    run_config={
        "resources": {
            "csv": {"config": {"csv_path": "data/transactions.csv"}},
            "archive": {"config": {"archive_path": "archive/"}},
        }
    },
)


defs = Definitions(
    jobs=[regulatory_report_router],
    schedules=[daily_schedule],
    resources={"csv": CSVResource, "archive": ArchiveResource},
)


if __name__ == "__main__":
    result = regulatory_report_router.execute_in_process(
        run_config={
            "resources": {
                "csv": {"config": {"csv_path": "data/transactions.csv"}},
                "archive": {"config": {"archive_path": "archive/"}},
            }
        }
    )
    assert result.success