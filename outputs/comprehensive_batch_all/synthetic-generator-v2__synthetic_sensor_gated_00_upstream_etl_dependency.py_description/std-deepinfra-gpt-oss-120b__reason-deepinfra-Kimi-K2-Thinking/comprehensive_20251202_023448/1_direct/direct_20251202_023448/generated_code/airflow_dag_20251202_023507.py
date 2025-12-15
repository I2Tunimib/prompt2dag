from datetime import datetime, timedelta
import logging
import os

import pandas as pd
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-team@example.com"],
}


def load_sales_csv(**context):
    """
    Load aggregated sales CSV data, validate its structure,
    and push the dataframe to XCom for downstream tasks.
    """
    csv_path = "/path/to/aggregated_sales.csv"  # Adjust to actual location
    if not os.path.isfile(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    df = pd.read_csv(csv_path)

    required_columns = {"date", "region", "product", "revenue"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in CSV: {missing}")

    # Push a serializable representation to XCom
    context["ti"].xcom_push(key="sales_df", value=df.to_dict())
    logging.info("Loaded and validated sales CSV with %d rows.", len(df))


def generate_dashboard(**context):
    """
    Generate an executive dashboard using the sales data
    retrieved from XCom. The dashboard is saved as an HTML file.
    """
    sales_data = context["ti"].xcom_pull(key="sales_df", task_ids="load_sales_csv")
    if not sales_data:
        raise ValueError("Sales data not found in XCom.")

    df = pd.DataFrame(sales_data)

    # Placeholder for real dashboard generation logic
    output_path = "/path/to/executive_dashboard.html"  # Adjust to desired location
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w") as f:
        f.write("<html><body><h1>Executive Sales Dashboard</h1></body></html>")

    logging.info("Dashboard generated at %s", output_path)


with DAG(
    dag_id="executive_sales_dashboard",
    default_args=default_args,
    description="Generate daily executive sales dashboard after aggregation.",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["sales", "dashboard"],
) as dag:
    wait_for_sales_aggregation = ExternalTaskSensor(
        task_id="wait_for_sales_aggregation",
        external_dag_id="daily_sales_aggregation",
        external_task_id=None,
        allowed_states=["success"],
        mode="reschedule",
        poke_interval=60,
        timeout=3600,
    )

    load_sales = PythonOperator(
        task_id="load_sales_csv",
        python_callable=load_sales_csv,
    )

    generate_dash = PythonOperator(
        task_id="generate_dashboard",
        python_callable=generate_dashboard,
    )

    wait_for_sales_aggregation >> load_sales >> generate_dash