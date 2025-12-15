from datetime import datetime, timedelta
import logging
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["data-team@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def load_sales_csv(**context):
    """Load and validate the aggregated sales CSV produced by the upstream DAG."""
    csv_path = Path("/opt/airflow/data/aggregated_sales.csv")
    if not csv_path.is_file():
        raise FileNotFoundError(f"Aggregated sales CSV not found at {csv_path}")

    df = pd.read_csv(csv_path)

    required_columns = {"date", "region", "product", "revenue"}
    missing = required_columns.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in CSV: {missing}")

    # Push the dataframe (as JSON) to XCom for downstream tasks
    context["ti"].xcom_push(key="sales_df", value=df.to_json())
    logging.info("Loaded %d rows from %s", len(df), csv_path)


def generate_dashboard(**context):
    """Generate a simple executive dashboard using the loaded sales data."""
    ti = context["ti"]
    df_json = ti.xcom_pull(key="sales_df", task_ids="load_sales_csv")
    if not df_json:
        raise ValueError("Sales data not found in XCom")

    df = pd.read_json(df_json)

    total_revenue = df["revenue"].sum()
    logging.info("Total revenue calculated: %s", total_revenue)

    report_path = Path("/opt/airflow/reports/executive_dashboard.txt")
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w") as f:
        f.write(f"Executive Sales Dashboard\n")
        f.write(f"Date: {datetime.utcnow().strftime('%Y-%m-%d')}\n")
        f.write(f"Total Revenue: {total_revenue:,.2f}\n")

    logging.info("Dashboard written to %s", report_path)


with DAG(
    dag_id="executive_sales_dashboard",
    description="Generate daily executive sales dashboard after aggregation",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["sales", "dashboard"],
) as dag:
    wait_for_sales_aggregation = ExternalTaskSensor(
        task_id="wait_for_sales_aggregation",
        external_dag_id="daily_sales_aggregation",
        external_task_id="aggregate_sales",
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        mode="reschedule",
        poke_interval=60,
        timeout=3600,
    )

    load_sales = PythonOperator(
        task_id="load_sales_csv",
        python_callable=load_sales_csv,
        provide_context=True,
    )

    generate_dash = PythonOperator(
        task_id="generate_dashboard",
        python_callable=generate_dashboard,
        provide_context=True,
    )

    wait_for_sales_aggregation >> load_sales >> generate_dash