from datetime import datetime, timedelta
import logging
import pandas as pd
import matplotlib.pyplot as plt
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['admin@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'daily_sales_report_pipeline',
    default_args=default_args,
    description='Daily sales reporting pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sales', 'reporting'],
) as dag:

    def query_sales_data(**context):
        """Extract daily sales data from PostgreSQL aggregated by product."""
        execution_date = context['execution_date']
        hook = PostgresHook(postgres_conn_id='postgres_default')
        
        sql = """
        SELECT product_name, SUM(quantity) as total_quantity, SUM(amount) as total_sales
        FROM sales
        WHERE DATE(sale_date) = DATE(%s)
        GROUP BY product_name
        ORDER BY product_name
        """
        
        records = hook.get_records(sql, parameters=[execution_date])
        
        if not records:
            logger.warning(f"No sales data found for {execution_date}")
            return []
        
        columns = ['product_name', 'total_quantity', 'total_sales']
        return [dict(zip(columns, row)) for row in records]

    def transform_to_csv(**context):
        """Transform query results into a CSV file."""
        data = context['task_instance'].xcom_pull(task_ids='query_sales_data')
        
        df = pd.DataFrame(data) if data else pd.DataFrame(columns=['product_name', 'total_quantity', 'total_sales'])
        
        csv_path = '/tmp/sales_report.csv'
        df.to_csv(csv_path, index=False)
        logger.info(f"CSV report saved to {csv_path}")
        
        return csv_path

    def generate_pdf_chart(**context):
        """Generate PDF bar chart visualizing sales by product."""
        csv_path = '/tmp/sales_report.csv'
        df = pd.read_csv(csv_path)
        
        plt.figure(figsize=(12, 8))
        
        if df.empty:
            plt.text(0.5, 0.5, 'No Data Available', ha='center', va='center')
        else:
            plt.bar(df['product_name'], df['total_sales'])
            plt.xlabel('Product Name')
            plt.ylabel('Total Sales')
            plt.title(f'Daily Sales by Product - {context["execution_date"].strftime("%Y-%m-%d")}')
            plt.xticks(rotation=45, ha='right')
        
        plt.tight_layout()
        pdf_path = '/tmp/sales_chart.pdf'
        plt.savefig(pdf_path, format='pdf')
        plt.close()
        logger.info(f"PDF chart saved to {pdf_path}")
        
        return pdf_path

    query_sales_data_task = PythonOperator(
        task_id='query_sales_data',
        python_callable=query_sales_data,
    )

    transform_to_csv_task = PythonOperator(
        task_id='transform_to_csv',
        python_callable=transform_to_csv,
    )

    generate_pdf_chart_task = PythonOperator(
        task_id='generate_pdf_chart',
        python_callable=generate_pdf_chart,
    )

    email_sales_report_task = EmailOperator(
        task_id='email_sales_report',
        to='management@company.com',
        subject='Daily Sales Report - {{ ds }}',
        html_content="""
        <h3>Daily Sales Report</h3>
        <p>Please find attached the daily sales report for <strong>{{ ds }}</strong>.</p>
        """,
        files=['/tmp/sales_report.csv', '/tmp/sales_chart.pdf'],
    )

    query_sales_data_task >> transform_to_csv_task >> generate_pdf_chart_task >> email_sales_report_task