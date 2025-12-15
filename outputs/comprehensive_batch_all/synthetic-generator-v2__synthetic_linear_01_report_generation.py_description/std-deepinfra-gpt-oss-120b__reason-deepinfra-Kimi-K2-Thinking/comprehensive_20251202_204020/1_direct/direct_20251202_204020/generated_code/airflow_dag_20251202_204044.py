"""Daily sales reporting DAG.

Extracts sales data from PostgreSQL, creates CSV and PDF reports,
and emails them to management. Runs once per day.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.email.operators.email import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def query_sales_data(**kwargs):
    """Execute a PostgreSQL query and return a pandas DataFrame."""
    execution_date = kwargs["ds"]  # YYYY-MM-DD string
    sql = """
        SELECT
            product_id,
            product_name,
            SUM(quantity) AS total_quantity,
            SUM(total_price) AS total_sales
        FROM sales
        WHERE sale_date = %(ds)s
        GROUP BY product_id, product_name;
    """
    hook = PostgresHook(postgres_conn_id="postgres_default")
    df = hook.get_pandas_df(sql, parameters={"ds": execution_date})
    return df


def transform_to_csv(**kwargs):
    """Write the DataFrame from the previous task to a CSV file."""
    ti = kwargs["ti"]
    df = ti.xcom_pull(task_ids="query_sales_data")
    csv_path = "/tmp/sales_report.csv"
    df.to_csv(csv_path, index=False)
    return csv_path


def generate_pdf_chart(**kwargs):
    """Create a PDF bar chart from the CSV file."""
    ti = kwargs["ti"]
    csv_path = ti.xcom_pull(task_ids="transform_to_csv")
    import pandas as pd
    import matplotlib.pyplot as plt

    df = pd.read_csv(csv_path)

    plt.figure(figsize=(10, 6))
    plt.bar(df["product_name"], df["total_sales"])
    plt.xlabel("Product")
    plt.ylabel("Total Sales")
    plt.title("Daily Sales by Product")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    pdf_path = "/tmp/sales_chart.pdf"
    plt.savefig(pdf_path)
    plt.close()
    return pdf_path


default_args = {
    "owner": "airflow",
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
    query = PythonOperator(
        task_id="query_sales_data",
        python_callable=query_sales_data,
        provide_context=True,
    )

    csv = PythonOperator(
        task_id="transform_to_csv",
        python_callable=transform_to_csv,
    )

    pdf = PythonOperator(
        task_id="generate_pdf_chart",
        python_callable=generate_pdf_chart,
    )

    email = EmailOperator(
        task_id="email_sales_report",
        to="management@company.com",
        subject="Daily Sales Report {{ ds }}",
        html_content="Please find attached the daily sales report.",
        files=["/tmp/sales_report.csv", "/tmp/sales_chart.pdf"],
    )

    query >> csv >> pdf >> email