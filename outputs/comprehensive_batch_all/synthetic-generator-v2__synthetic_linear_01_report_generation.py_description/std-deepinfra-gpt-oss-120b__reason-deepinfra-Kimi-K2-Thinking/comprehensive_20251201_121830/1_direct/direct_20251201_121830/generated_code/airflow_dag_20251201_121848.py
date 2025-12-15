from datetime import datetime, timedelta

import pandas as pd
import matplotlib.pyplot as plt

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.email.operators.email import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def query_sales_data(**context):
    """
    Extract daily sales data aggregated by product from PostgreSQL.
    Returns a list of dictionaries via XCom.
    """
    ds = context["ds"]  # execution date in YYYY-MM-DD format
    sql = f"""
        SELECT product, SUM(amount) AS total_sales
        FROM sales
        WHERE sale_date = '{ds}'
        GROUP BY product;
    """
    hook = PostgresHook(postgres_conn_id="postgres_default")
    df = hook.get_pandas_df(sql)
    return df.to_dict(orient="records")


def transform_to_csv(**context):
    """
    Transform query results into a CSV file stored at /tmp/sales_report.csv.
    """
    records = context["ti"].xcom_pull(task_ids="query_sales_data")
    if not records:
        raise ValueError("No data received from query_sales_data task.")
    df = pd.DataFrame(records)
    csv_path = "/tmp/sales_report.csv"
    df.to_csv(csv_path, index=False)


def generate_pdf_chart(**context):
    """
    Generate a PDF bar chart visualizing sales by product.
    The chart is saved at /tmp/sales_chart.pdf.
    """
    csv_path = "/tmp/sales_report.csv"
    pdf_path = "/tmp/sales_chart.pdf"
    df = pd.read_csv(csv_path)

    plt.figure(figsize=(10, 6))
    plt.bar(df["product"], df["total_sales"], color="#4c72b0")
    plt.xlabel("Product")
    plt.ylabel("Total Sales")
    plt.title("Daily Sales by Product")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(pdf_path)
    plt.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["alerts@company.com"],
}

with DAG(
    dag_id="daily_sales_reporting",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["sales", "reporting"],
) as dag:
    extract_task = PythonOperator(
        task_id="query_sales_data",
        python_callable=query_sales_data,
        provide_context=True,
    )

    csv_task = PythonOperator(
        task_id="transform_to_csv",
        python_callable=transform_to_csv,
        provide_context=True,
    )

    pdf_task = PythonOperator(
        task_id="generate_pdf_chart",
        python_callable=generate_pdf_chart,
        provide_context=True,
    )

    email_task = EmailOperator(
        task_id="email_sales_report",
        to="management@company.com",
        subject="Daily Sales Report",
        html_content="Please find attached the daily sales CSV report and PDF chart.",
        files=["/tmp/sales_report.csv", "/tmp/sales_chart.pdf"],
    )

    extract_task >> csv_task >> pdf_task >> email_task