import os
from datetime import datetime, timedelta

import pandas as pd
import matplotlib.pyplot as plt

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def query_sales_data(**context):
    """
    Execute a PostgreSQL query to retrieve daily sales aggregated by product.
    The result is pushed to XCom as a list of dictionaries.
    """
    hook = PostgresHook(postgres_conn_id="postgres_default")
    sql = """
        SELECT product_id, SUM(amount) AS total_sales
        FROM sales
        WHERE sale_date = '{{ ds }}'
        GROUP BY product_id
        ORDER BY product_id;
    """
    # Render the template variable {{ ds }} using Airflow's context
    rendered_sql = context["ti"].render_template(sql, context)
    df = hook.get_pandas_df(rendered_sql)
    # Push dataframe to XCom (as JSON-serializable list of dicts)
    records = df.to_dict(orient="records")
    context["ti"].xcom_push(key="sales_data", value=records)


def transform_to_csv(**context):
    """
    Convert the sales data from XCom into a CSV file stored in /tmp.
    """
    records = context["ti"].xcom_pull(key="sales_data", task_ids="query_sales_data")
    if not records:
        raise ValueError("No sales data found in XCom.")
    df = pd.DataFrame(records)
    csv_path = "/tmp/sales_report.csv"
    df.to_csv(csv_path, index=False)
    # Push CSV path for downstream tasks
    context["ti"].xcom_push(key="csv_path", value=csv_path)


def generate_pdf_chart(**context):
    """
    Generate a PDF bar chart visualizing sales by product.
    """
    csv_path = context["ti"].xcom_pull(key="csv_path", task_ids="transform_to_csv")
    if not csv_path or not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found at {csv_path}")
    df = pd.read_csv(csv_path)

    plt.figure(figsize=(10, 6))
    plt.bar(df["product_id"].astype(str), df["total_sales"], color="steelblue")
    plt.xlabel("Product ID")
    plt.ylabel("Total Sales")
    plt.title("Daily Sales by Product")
    plt.tight_layout()

    pdf_path = "/tmp/sales_chart.pdf"
    plt.savefig(pdf_path, format="pdf")
    plt.close()

    # Push PDF path for downstream tasks
    context["ti"].xcom_push(key="pdf_path", value=pdf_path)


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

    query_task = PythonOperator(
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
        files=[
            "{{ ti.xcom_pull(key='csv_path', task_ids='transform_to_csv') }}",
            "{{ ti.xcom_pull(key='pdf_path', task_ids='generate_pdf_chart') }}",
        ],
    )

    query_task >> csv_task >> pdf_task >> email_task