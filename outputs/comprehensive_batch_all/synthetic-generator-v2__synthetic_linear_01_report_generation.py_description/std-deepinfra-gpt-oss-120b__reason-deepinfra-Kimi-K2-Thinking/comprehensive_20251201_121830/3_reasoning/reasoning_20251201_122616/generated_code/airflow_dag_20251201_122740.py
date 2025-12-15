from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import matplotlib.pyplot as plt
import os
import logging

# Configure matplotlib for non-interactive environment
plt.switch_backend('Agg')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['admin@company.com'],  # Configure with actual admin email
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'daily_sales_report_pipeline',
    default_args=default_args,
    description='Daily sales reporting pipeline extracting from PostgreSQL, generating CSV and PDF, and emailing to management',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sales', 'reporting', 'daily'],
) as dag:

    def query_sales_data(**kwargs):
        """Extract daily sales data from PostgreSQL aggregated by product for execution date."""
        try:
            execution_date = kwargs['execution_date'].date()
            logging.info(f"Querying sales data for date: {execution_date}")
            
            hook = PostgresHook(postgres_conn_id='postgres_default')
            sql = """
                SELECT product_name, 
                       SUM(quantity) as total_quantity, 
                       SUM(amount) as total_sales
                FROM sales
                WHERE DATE(sale_date) = %s
                GROUP BY product_name
                ORDER BY product_name
            """
            records = hook.get_records(sql, parameters=(execution_date,))
            
            if not records:
                logging.warning("No sales data found for the given date")
                return []
                
            data = [
                {
                    'product_name': r[0],
                    'total_quantity': float(r[1]) if r[1] else 0,
                    'total_sales': float(r[2]) if r[2] else 0
                }
                for r in records
            ]
            
            kwargs['ti'].xcom_push(key='sales_data', value=data)
            logging.info(f"Successfully retrieved {len(data)} records")
            return data
            
        except Exception as e:
            logging.error(f"Error in query_sales_data: {str(e)}")
            raise

    def transform_to_csv(**kwargs):
        """Transform query results into CSV file."""
        try:
            ti = kwargs['ti']
            data = ti.xcom_pull(task_ids='query_sales_data', key='sales_data')
            
            if not data:
                logging.warning("No data received from query_sales_data task")
                data = []
            
            df = pd.DataFrame(data)
            csv_path = '/tmp/sales_report.csv'
            df.to_csv(csv_path, index=False)
            
            logging.info(f"CSV file created at {csv_path} with {len(df)} records")
            return csv_path
            
        except Exception as e:
            logging.error(f"Error in transform_to_csv: {str(e)}")
            raise

    def generate_pdf_chart(**kwargs):
        """Create PDF bar chart visualizing sales by product."""
        try:
            csv_path = '/tmp/sales_report.csv'
            
            if not os.path.exists(csv_path):
                raise FileNotFoundError(f"CSV file not found at {csv_path}")
            
            df = pd.read_csv(csv_path)
            
            if df.empty:
                logging.warning("No data in CSV file, creating empty chart")
                plt.figure(figsize=(10, 6))
                plt.text(0.5, 0.5, 'No sales data available', 
                        ha='center', va='center', transform=plt.gca().transAxes)
            else:
                plt.figure(figsize=(12, 7))
                plt.bar(df['product_name'], df['total_sales'], color='skyblue')
                plt.xlabel('Product', fontsize=12)
                plt.ylabel('Total Sales', fontsize=12)
                plt.title(f'Daily Sales by Product - {kwargs["execution_date"].date()}', 
                         fontsize=14, fontweight='bold')
                plt.xticks(rotation=45, ha='right')
            
            plt.tight_layout()
            pdf_path = '/tmp/sales_chart.pdf'
            plt.savefig(pdf_path, dpi=300, bbox_inches='tight')
            plt.close()
            
            logging.info(f"PDF chart created at {pdf_path}")
            return pdf_path
            
        except Exception as e:
            logging.error(f"Error in generate_pdf_chart: {str(e)}")
            raise

    query_sales_data_task = PythonOperator(
        task_id='query_sales_data',
        python_callable=query_sales_data,
        provide_context=True,
    )

    transform_to_csv_task = PythonOperator(
        task_id='transform_to_csv',
        python_callable=transform_to_csv,
        provide_context=True,
    )

    generate_pdf_chart_task = PythonOperator(
        task_id='generate_pdf_chart',
        python_callable=generate_pdf_chart,
        provide_context=True,
    )

    email_sales_report_task = EmailOperator(
        task_id='email_sales_report',
        to='management@company.com',
        subject='Daily Sales Report - {{ ds }}',
        html_content="""
        <h3>Daily Sales Report</h3>
        <p>Please find attached the daily sales report (CSV) and chart (PDF) for {{ ds }}.</p>
        <p>This is an automated message from the sales reporting pipeline.</p>
        """,
        files=['/tmp/sales_report.csv', '/tmp/sales_chart.pdf'],
        mime_charset='utf-8',
    )

    query_sales_data_task >> transform_to_csv_task >> generate_pdf_chart_task >> email_sales_report_task