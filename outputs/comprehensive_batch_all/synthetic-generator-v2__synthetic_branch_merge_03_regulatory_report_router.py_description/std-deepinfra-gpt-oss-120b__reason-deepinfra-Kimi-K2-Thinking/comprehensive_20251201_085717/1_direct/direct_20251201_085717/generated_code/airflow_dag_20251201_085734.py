"""Regulatory report router DAG.

Processes financial transaction CSV data, routes records to FATCA or IRS reporting
based on account type, and archives the generated reports.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


def read_csv(**kwargs):
    """Read transaction CSV and push records to XCom."""
    import os
    import pandas as pd

    file_path = "/path/to/transactions.csv"  # Adjust to actual location
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
    else:
        # Empty DataFrame with expected columns if file is missing
        df = pd.DataFrame(columns=["account_id", "account_type", "amount"])

    records = df.to_dict(orient="records")
    kwargs["ti"].xcom_push(key="transactions", value=records)


def account_check(**kwargs):
    """Determine which reporting branches should run."""
    ti = kwargs["ti"]
    records = ti.xcom_pull(key="transactions", task_ids="read_csv") or []

    has_international = any(r.get("account_type") == "international" for r in records)
    has_domestic = any(r.get("account_type") == "domestic" for r in records)

    branches = []
    if has_international:
        branches.append("route_to_fatca")
    if has_domestic:
        branches.append("route_to_irs")
    if not branches:
        # No relevant records – proceed directly to archiving
        branches.append("archive_reports")
    return branches


def route_to_fatca(**kwargs):
    """Generate FATCA XML reports for international accounts."""
    ti = kwargs["ti"]
    records = ti.xcom_pull(key="transactions", task_ids="read_csv") or []

    intl_records = [r for r in records if r.get("account_type") == "international"]
    reports = [
        f"<report><account>{r['account_id']}</account></report>" for r in intl_records
    ]

    ti.xcom_push(key="fatca_reports", value=reports)


def route_to_irs(**kwargs):
    """Generate IRS 1099 data for domestic accounts."""
    ti = kwargs["ti"]
    records = ti.xcom_pull(key="transactions", task_ids="read_csv") or []

    dom_records = [r for r in records if r.get("account_type") == "domestic"]
    reports = [
        f"1099 for account {r['account_id']}: amount {r['amount']}" for r in dom_records
    ]

    ti.xcom_push(key="irs_reports", value=reports)


def archive_reports(**kwargs):
    """Merge all reports, compress, and store in a secure archive."""
    ti = kwargs["ti"]
    fatca = ti.xcom_pull(key="fatca_reports", task_ids="route_to_fatca") or []
    irs = ti.xcom_pull(key="irs_reports", task_ids="route_to_irs") or []
    all_reports = fatca + irs

    # Simple archival: write JSON list to a file named with execution date
    import json
    import os

    archive_dir = "/path/to/archive"
    os.makedirs(archive_dir, exist_ok=True)
    archive_path = os.path.join(
        archive_dir, f"reports_{kwargs['ds_nodash']}.json"
    )
    with open(archive_path, "w") as f:
        json.dump(all_reports, f)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
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

    fatca_task = PythonOperator(
        task_id="route_to_fatca",
        python_callable=route_to_fatca,
    )

    irs_task = PythonOperator(
        task_id="route_to_irs",
        python_callable=route_to_irs,
    )

    archive_task = PythonOperator(
        task_id="archive_reports",
        python_callable=archive_reports,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Define dependencies
    read_csv_task >> account_check_task
    account_check_task >> [fatca_task, irs_task, archive_task]
    [fatca_task, irs_task] >> archive_task