from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email


# Configure SMTP settings in Airflow for email notifications to function
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['admin@company.com'],  # Update with actual admin email
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def query_sales_data(**context):
    """Extract daily sales data from PostgreSQL aggregated by product."""
    execution_date = context['logical_date'].date()
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    sql = """
    SELECT product, SUM(sales) as total_sales
    FROM sales
    WHERE date = %s
    GROUP BY product
    ORDER BY product
    """
    
    records = hook.get_records(sql, parameters=[execution_date])
    return [{'product': row[0], 'total_sales': float(row[1])} for row in records]


def transform_to_csv(**context):
    """Transform query results into a CSV file."""
    data = context['task_instance'].xcom_pull(task_ids='query_sales_data')
    
    if not data:
        raise ValueError("No data retrieved from query_sales_data task")
    
    df = pd.DataFrame(data)
    csv_path = '/tmp/sales_report.csv'
    df.to_csv(csv_path, index=False)
    
    return csv_path


def generate_pdf_chart(**context):
    """Create a PDF bar chart visualizing sales by product."""
    data = context['task_instance'].xcom_pull(task_ids='query_sales_data')
    
    if not data:
        raise ValueError("No data retrieved from query_sales_data task")
    
    df = pd.DataFrame(data)
    
    plt.figure(figsize=(12, 7))
    plt.bar(df['product'], df['total_sales'], color='skyblue')
    plt.xlabel('Product', fontsize=12)
    plt.ylabel('Total Sales', fontsize=12)
    plt.title(f'Sales by Product - {context["logical_date"].date()}', fontsize=14)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    
    pdf_path = '/tmp/sales_chart.pdf'
    plt.savefig(pdf_path, format='pdf')
    plt.close()
    
    return pdf_path


def email_sales_report(**context):
    """Send email with CSV and PDF attachments to management."""
    csv_path = context['task_instance'].xcom_pull(task_ids='transform_to_csv')
    pdf_path = context['task_instance'].xcom_pull(task_ids='generate_pdf_chart')
    
    if not csv_path or not pdf_path:
        raise ValueError("Required file paths not found in XCom")
    
    subject = f'Daily Sales Report - {context["logical_date"].date()}'
    html_content = """
    <h3>Daily Sales Report</h3>
    <p>Please find attached the daily sales report (CSV) and chart (PDF).</p>
    """
    
    send_email(
        to=['management@company.com'],
        subject=subject,
        html_content=html_content,
        files=[csv_path, pdf_path]
    )


with DAG(
    'daily_sales_reporting_pipeline',
    default_args=default_args,
    description='Daily sales reporting pipeline extracting from PostgreSQL, transforming to CSV/PDF, and emailing management',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['sales', 'reporting', 'daily'],
) as dag:
    
    query_task = PythonOperator(
        task_id='query_sales_data',
        python_callable=query_sales_data,
    )
    
    csv_task = PythonOperator(
        task_id='transform_to_csv',
        python_callable=transform_to_csv,
    )
    
    pdf_task = PythonOperator(
        task_id='generate_pdf_chart',
        python_callable=generate_pdf_chart,
    )
    
    email_task = PythonOperator(
        task_id='email_sales_report',
        python_callable=email_sales_report,
    )
    
    query_task >> csv_task >> pdf_task >> email_task