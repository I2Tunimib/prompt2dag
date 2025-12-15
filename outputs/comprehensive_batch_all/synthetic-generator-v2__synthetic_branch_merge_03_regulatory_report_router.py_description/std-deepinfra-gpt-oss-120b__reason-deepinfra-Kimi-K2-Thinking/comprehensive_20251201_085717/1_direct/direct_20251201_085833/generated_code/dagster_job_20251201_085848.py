from datetime import timedelta
import csv
import json
import os
import pathlib
import zipfile
from typing import List, Tuple

from dagster import (
    DefaultScheduleStatus,
    In,
    Out,
    RetryPolicy,
    ScheduleDefinition,
    job,
    op,
    schedule,
)


@op(
    config_schema={"csv_path": str},
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    out=Out(List[dict]),
    description="Read financial transaction CSV file.",
)
def read_csv(context) -> List[dict]:
    csv_path = context.op_config["csv_path"]
    context.log.info(f"Reading CSV file from {csv_path}")
    transactions: List[dict] = []
    with open(csv_path, newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            transactions.append(row)
    context.log.info(f"Loaded {len(transactions)} transactions")
    return transactions


@op(
    out={"international": Out(List[dict]), "domestic": Out(List[dict])},
    description="Split transactions by account type for routing.",
)
def account_check(
    context, transactions: List[dict]
) -> Tuple[List[dict], List[dict]]:
    international: List[dict] = []
    domestic: List[dict] = []
    for txn in transactions:
        account_type = txn.get("account_type", "").lower()
        if account_type == "international":
            international.append(txn)
        else:
            domestic.append(txn)
    context.log.info(
        f"International accounts: {len(international)}, Domestic accounts: {len(domestic)}"
    )
    return international, domestic


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    out=Out(List[str]),
    description="Process international accounts through FATCA and generate XML reports.",
)
def route_to_fatca(context, international: List[dict]) -> List[str]:
    output_dir = pathlib.Path("output/fatca")
    output_dir.mkdir(parents=True, exist_ok=True)
    report_paths: List[str] = []
    for idx, txn in enumerate(international, start=1):
        report_path = output_dir / f"fatca_report_{idx}.xml"
        # Minimal XML stub
        xml_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<FATCAReport>
    <TransactionID>{txn.get('transaction_id', '')}</TransactionID>
    <Amount>{txn.get('amount', '')}</Amount>
    <AccountType>{txn.get('account_type', '')}</AccountType>
</FATCAReport>"""
        report_path.write_text(xml_content, encoding="utf-8")
        report_paths.append(str(report_path))
    context.log.info(f"Generated {len(report_paths)} FATCA reports")
    return report_paths


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    out=Out(List[str]),
    description="Process domestic accounts through IRS and generate Form 1099 JSON data.",
)
def route_to_irs(context, domestic: List[dict]) -> List[str]:
    output_dir = pathlib.Path("output/irs")
    output_dir.mkdir(parents=True, exist_ok=True)
    report_paths: List[str] = []
    for idx, txn in enumerate(domestic, start=1):
        report_path = output_dir / f"irs_report_{idx}.json"
        # Minimal JSON stub representing Form 1099 data
        json_content = {
            "TransactionID": txn.get("transaction_id", ""),
            "Amount": txn.get("amount", ""),
            "AccountType": txn.get("account_type", ""),
        }
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(json_content, f, indent=2)
        report_paths.append(str(report_path))
    context.log.info(f"Generated {len(report_paths)} IRS reports")
    return report_paths


@op(
    config_schema={"archive_path": str},
    description="Merge and archive all regulatory reports into a compressed file.",
)
def archive_reports(
    context,
    fatca_reports: List[str],
    irs_reports: List[str],
) -> str:
    archive_root = pathlib.Path(context.op_config["archive_path"])
    archive_root.mkdir(parents=True, exist_ok=True)
    archive_file = archive_root / "regulatory_reports.zip"
    context.log.info(f"Creating archive at {archive_file}")

    with zipfile.ZipFile(archive_file, "w", zipfile.ZIP_DEFLATED) as zipf:
        for report_path in fatca_reports + irs_reports:
            zipf.write(report_path, arcname=os.path.basename(report_path))

    context.log.info(f"Archive created with {len(fatca_reports) + len(irs_reports)} files")
    return str(archive_file)


@job(description="Regulatory report router with branching for FATCA and IRS processing.")
def regulatory_report_router():
    transactions = read_csv()
    international, domestic = account_check(transactions)
    fatca_reports = route_to_fatca(international)
    irs_reports = route_to_irs(domestic)
    archive_reports(fatca_reports, irs_reports)


daily_regulatory_report_router_schedule = ScheduleDefinition(
    job=regulatory_report_router,
    cron_schedule="0 0 * * *",  # Daily at midnight UTC
    default_status=DefaultScheduleStatus.RUNNING,
    run_config={
        "ops": {
            "read_csv": {"config": {"csv_path": "data/transactions.csv"}},
            "archive_reports": {"config": {"archive_path": "archive/"}},
        }
    },
    description="Daily execution of the regulatory report router pipeline.",
)


if __name__ == "__main__":
    result = regulatory_report_router.execute_in_process(
        run_config={
            "ops": {
                "read_csv": {"config": {"csv_path": "data/transactions.csv"}},
                "archive_reports": {"config": {"archive_path": "archive/"}},
            }
        }
    )
    assert result.success