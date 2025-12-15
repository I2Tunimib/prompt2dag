from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.smtp.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['management@company.com']
}

def query_sales_data(**kwargs):
    ti = kwargs['ti']
    query = """
    SELECT product, SUM(sales) as total_sales
    FROM sales
    WHERE date = '{{ ds }}'
    GROUP BY product;
    """
    df = pd.read_sql_query(query, con=kwargs['postgres_conn_id'])
    ti.xcom_push(key='sales_data', value=df)

def transform_to_csv(**kwargs):
    ti = kwargs['ti']
    sales_data = ti.xcom_pull(task_ids='query_sales_data', key='sales_data')
    csv_path = '/tmp/sales_report.csv'
    sales_data.to_csv(csv_path, index=False)
    return csv_path

def generate_pdf_chart(**kwargs):
    ti = kwargs['ti']
    sales_data = ti.xcom_pull(task_ids='query_sales_data', key='sales_data')
    pdf_path = '/tmp/sales_chart.pdf'
    plt.figure(figsize=(10, 6))
    plt.bar(sales_data['product'], sales_data['total_sales'])
    plt.xlabel('Product')
    plt.ylabel('Total Sales')
    plt.title('Daily Sales by Product')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(pdf_path)
    return pdf_path

def email_sales_report(**kwargs):
    ti = kwargs['ti']
    csv_path = ti.xcom_pull(task_ids='transform_to_csv')
    pdf_path = ti.xcom_pull(task_ids='generate_pdf_chart')
    email_operator = EmailOperator(
        task_id='email_sales_report',
        to='management@company.com',
        subject='Daily Sales Report',
        html_content='Please find attached the daily sales report.',
        files=[csv_path, pdf_path]
    )
    email_operator.execute(context=kwargs)

with DAG(
    'daily_sales_reporting',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    query_sales_data_task = PythonOperator(
        task_id='query_sales_data',
        python_callable=query_sales_data,
        op_kwargs={'postgres_conn_id': 'postgres_default'}
    )

    transform_to_csv_task = PythonOperator(
        task_id='transform_to_csv',
        python_callable=transform_to_csv
    )

    generate_pdf_chart_task = PythonOperator(
        task_id='generate_pdf_chart',
        python_callable=generate_pdf_chart
    )

    email_sales_report_task = PythonOperator(
        task_id='email_sales_report',
        python_callable=email_sales_report
    )

    query_sales_data_task >> transform_to_csv_task >> generate_pdf_chart_task >> email_sales_report_task