"""Executive sales dashboard DAG."""

from datetime import datetime, timedelta
import logging
import os

import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

# Default arguments applied to all tasks
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-team@example.com"],
}

# Paths used by the pipeline
AGGREGATED_SALES_CSV = "/opt/airflow/data/aggregated_sales.csv"
DASHBOARD_OUTPUT = "/opt/airflow/reports/executive_dashboard.html"


def load_sales_csv(**kwargs):
    """Load and validate the aggregated sales CSV produced by the upstream DAG."""
    logger = logging.getLogger("airflow.task")
    if not os.path.exists(AGGREGATED_SALES_CSV):
        raise FileNotFoundError(f"Aggregated sales file not found: {AGGREGATED_SALES_CSV}")

    df = pd.read_csv(AGGREGATED_SALES_CSV)
    required_columns = {"date", "region", "product", "revenue", "units_sold"}
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in sales CSV: {missing}")

    if df.empty:
        raise ValueError("Aggregated sales CSV is empty")

    # Push the dataframe (as JSON) to XCom for downstream consumption
    kwargs["ti"].xcom_push(key="sales_data", value=df.to_json(date_format="iso"))
    logger.info("Loaded %d rows from %s", len(df), AGGREGATED_SALES_CSV)


def generate_dashboard(**kwargs):
    """Generate an executive dashboard from the loaded sales data."""
    logger = logging.getLogger("airflow.task")
    ti = kwargs["ti"]
    sales_json = ti.xcom_pull(key="sales_data", task_ids="load_sales_csv")
    if not sales_json:
        raise ValueError("No sales data found in XCom from load_sales_csv")

    df = pd.read_json(sales_json)

    # Example metric: total revenue
    total_revenue = df["revenue"].sum()
    html_content = f"""
    <html>
        <head><title>Executive Sales Dashboard</title></head>
        <body>
            <h1>Executive Sales Dashboard</h1>
            <p>Total Revenue: ${total_revenue:,.2f}</p>
        </body>
    </html>
    """

    os.makedirs(os.path.dirname(DASHBOARD_OUTPUT), exist_ok=True)
    with open(DASHBOARD_OUTPUT, "w") as f:
        f.write(html_content)

    logger.info("Dashboard generated at %s", DASHBOARD_OUTPUT)


with DAG(
    dag_id="executive_sales_dashboard",
    description="Generate daily executive sales dashboard after aggregation completes.",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    concurrency=1,
    tags=["sales", "dashboard"],
) as dag:
    wait_for_sales_aggregation = ExternalTaskSensor(
        task_id="wait_for_sales_aggregation",
        external_dag_id="daily_sales_aggregation",
        external_task_id=None,
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