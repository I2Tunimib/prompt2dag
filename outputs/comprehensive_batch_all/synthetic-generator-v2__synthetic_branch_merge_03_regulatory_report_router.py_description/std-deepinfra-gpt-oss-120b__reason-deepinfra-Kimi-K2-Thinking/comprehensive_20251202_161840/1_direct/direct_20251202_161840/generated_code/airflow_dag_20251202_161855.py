"""Regulatory report router DAG.

Processes financial transaction CSV data, routes transactions to FATCA or IRS reporting
based on account type, and archives the generated reports.
"""

from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


def read_csv(**kwargs):
    """Read transactions CSV and push data to XCom."""
    import csv
    import os
    import logging

    file_path = "/path/to/transactions.csv"  # Adjust to actual location
    transactions = []

    if os.path.exists(file_path):
        with open(file_path, newline="") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                transactions.append(row)
        logging.info("Read %d transaction rows.", len(transactions))
    else:
        logging.warning("CSV file %s not found. No data loaded.", file_path)

    kwargs["ti"].xcom_push(key="transactions", value=transactions)


def account_check(**kwargs):
    """Determine which reporting branches to follow based on account types."""
    ti = kwargs["ti"]
    transactions = ti.xcom_pull(key="transactions", task_ids="read_csv") or []

    has_international = any(
        row.get("account_type", "").lower() == "international" for row in transactions
    )
    has_domestic = any(
        row.get("account_type", "").lower() == "domestic" for row in transactions
    )

    branches = []
    if has_international:
        branches.append("route_to_fatca")
    if has_domestic:
        branches.append("route_to_irs")

    # If no relevant accounts, skip reporting and go straight to archive
    if not branches:
        branches.append("archive_reports")

    return branches


def route_to_fatca(**kwargs):
    """Process international accounts and generate FATCA XML reports."""
    import logging

    ti = kwargs["ti"]
    transactions = ti.xcom_pull(key="transactions", task_ids="read_csv") or []

    international = [
        row for row in transactions if row.get("account_type", "").lower() == "international"
    ]

    reports = [
        f"<report><transaction_id>{row.get('transaction_id')}</transaction_id></report>"
        for row in international
    ]

    logging.info("Generated %d FATCA reports.", len(reports))
    ti.xcom_push(key="fatca_reports", value=reports)


def route_to_irs(**kwargs):
    """Process domestic accounts and generate IRS 1099 data."""
    import logging

    ti = kwargs["ti"]
    transactions = ti.xcom_pull(key="transactions", task_ids="read_csv") or []

    domestic = [
        row for row in transactions if row.get("account_type", "").lower() == "domestic"
    ]

    reports = [
        f"1099_form_{row.get('transaction_id')}.json" for row in domestic
    ]

    logging.info("Generated %d IRS reports.", len(reports))
    ti.xcom_push(key="irs_reports", value=reports)


def archive_reports(**kwargs):
    """Merge all reports, compress, and store in secure archive."""
    import logging
    import json
    import os
    from datetime import datetime

    ti = kwargs["ti"]
    fatca_reports = ti.xcom_pull(key="fatca_reports", task_ids="route_to_fatca") or []
    irs_reports = ti.xcom_pull(key="irs_reports", task_ids="route_to_irs") or []

    all_reports = fatca_reports + irs_reports
    ds_nodash = kwargs["ds_nodash"]
    archive_path = f"/path/to/archive/reports_{ds_nodash}.zip"

    # Placeholder: just log the action; real implementation would create a zip file.
    logging.info(
        "Archiving %d reports to %s (execution date %s).",
        len(all_reports),
        archive_path,
        kwargs["execution_date"],
    )

    # Example of writing a manifest file (optional)
    manifest_path = f"/tmp/manifest_{ds_nodash}.json"
    with open(manifest_path, "w") as mf:
        json.dump({"reports": all_reports, "archive_path": archive_path}, mf)

    # Push archive location for downstream use
    ti.xcom_push(key="archive_path", value=archive_path)


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": days_ago(1),
}

with DAG(
    dag_id="regulatory_report_router",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["regulatory", "reporting"],
) as dag:
    read_csv_task = PythonOperator(
        task_id="read_csv",
        python_callable=read_csv,
    )

    account_check_task = BranchPythonOperator(
        task_id="account_check",
        python_callable=account_check,
    )

    route_to_fatca_task = PythonOperator(
        task_id="route_to_fatca",
        python_callable=route_to_fatca,
    )

    route_to_irs_task = PythonOperator(
        task_id="route_to_irs",
        python_callable=route_to_irs,
    )

    archive_reports_task = PythonOperator(
        task_id="archive_reports",
        python_callable=archive_reports,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Define dependencies
    read_csv_task >> account_check_task
    account_check_task >> [route_to_fatca_task, route_to_irs_task]
    route_to_fatca_task >> archive_reports_task
    route_to_irs_task >> archive_reports_task