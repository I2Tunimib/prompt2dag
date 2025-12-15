from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['admin@example.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def ingest_sales_data(region):
    """Ingest sales data from CSV for the specified region."""
    file_path = f'/path/to/{region}_sales.csv'
    df = pd.read_csv(file_path)
    return df.to_json()

def convert_currency(region, df_json):
    """Convert currency to USD for the specified region."""
    df = pd.read_json(df_json)
    if region == 'EU':
        df['amount_usd'] = df['amount'] * 1.1  # Example exchange rate
    elif region == 'APAC':
        df['amount_usd'] = df['amount'] * 0.007  # Example exchange rate
    else:
        df['amount_usd'] = df['amount']
    return df.to_json()

def aggregate_data(*dfs_json):
    """Aggregate data from all regions and generate the final report."""
    dfs = [pd.read_json(df_json) for df_json in dfs_json]
    combined_df = pd.concat(dfs)
    total_revenue = combined_df['amount_usd'].sum()
    report = f"Total Global Revenue: ${total_revenue:.2f}"
    with open('/path/to/global_revenue_report.csv', 'w') as f:
        f.write(report)
    return report

with DAG(
    'multi_region_ecommerce_pipeline',
    default_args=default_args,
    description='Multi-region ecommerce analytics pipeline',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    start_pipeline = EmptyOperator(task_id='start_pipeline')

    with TaskGroup('ingest_sales_data') as ingest_sales_data_group:
        ingest_us_east = PythonOperator(
            task_id='ingest_us_east',
            python_callable=ingest_sales_data,
            op_kwargs={'region': 'US-East'}
        )
        ingest_us_west = PythonOperator(
            task_id='ingest_us_west',
            python_callable=ingest_sales_data,
            op_kwargs={'region': 'US-West'}
        )
        ingest_eu = PythonOperator(
            task_id='ingest_eu',
            python_callable=ingest_sales_data,
            op_kwargs={'region': 'EU'}
        )
        ingest_apac = PythonOperator(
            task_id='ingest_apac',
            python_callable=ingest_sales_data,
            op_kwargs={'region': 'APAC'}
        )

    with TaskGroup('convert_currency') as convert_currency_group:
        convert_us_east = PythonOperator(
            task_id='convert_us_east',
            python_callable=convert_currency,
            op_kwargs={'region': 'US-East'},
            op_args=['{{ ti.xcom_pull(task_ids="ingest_sales_data.ingest_us_east") }}']
        )
        convert_us_west = PythonOperator(
            task_id='convert_us_west',
            python_callable=convert_currency,
            op_kwargs={'region': 'US-West'},
            op_args=['{{ ti.xcom_pull(task_ids="ingest_sales_data.ingest_us_west") }}']
        )
        convert_eu = PythonOperator(
            task_id='convert_eu',
            python_callable=convert_currency,
            op_kwargs={'region': 'EU'},
            op_args=['{{ ti.xcom_pull(task_ids="ingest_sales_data.ingest_eu") }}']
        )
        convert_apac = PythonOperator(
            task_id='convert_apac',
            python_callable=convert_currency,
            op_kwargs={'region': 'APAC'},
            op_args=['{{ ti.xcom_pull(task_ids="ingest_sales_data.ingest_apac") }}']
        )

    aggregate_data_task = PythonOperator(
        task_id='aggregate_data',
        python_callable=aggregate_data,
        op_args=[
            '{{ ti.xcom_pull(task_ids="convert_currency.convert_us_east") }}',
            '{{ ti.xcom_pull(task_ids="convert_currency.convert_us_west") }}',
            '{{ ti.xcom_pull(task_ids="convert_currency.convert_eu") }}',
            '{{ ti.xcom_pull(task_ids="convert_currency.convert_apac") }}'
        ]
    )

    end_pipeline = EmptyOperator(task_id='end_pipeline')

    start_pipeline >> ingest_sales_data_group >> convert_currency_group >> aggregate_data_task >> end_pipeline