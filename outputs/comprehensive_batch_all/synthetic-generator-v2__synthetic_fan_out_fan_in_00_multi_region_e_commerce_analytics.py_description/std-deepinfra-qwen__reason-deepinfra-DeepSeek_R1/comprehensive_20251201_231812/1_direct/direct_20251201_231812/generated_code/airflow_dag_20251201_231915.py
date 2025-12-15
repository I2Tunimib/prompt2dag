from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.email import EmailOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import timedelta, datetime
import pandas as pd
import requests

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
    file_path = f"/path/to/{region}_sales.csv"
    df = pd.read_csv(file_path)
    return df.to_json()

def convert_currency(region, from_currency, to_currency):
    """Convert currency of sales data from one currency to another."""
    if from_currency == to_currency:
        return ingest_sales_data(region)
    
    exchange_rate = get_exchange_rate(from_currency, to_currency)
    df = pd.read_json(ingest_sales_data(region))
    df['amount'] = df['amount'] * exchange_rate
    return df.to_json()

def get_exchange_rate(from_currency, to_currency):
    """Fetch the exchange rate from an external API."""
    url = f"https://api.exchangerate-api.com/v4/latest/{from_currency}"
    response = requests.get(url)
    data = response.json()
    return data['rates'][to_currency]

def aggregate_data(**context):
    """Aggregate data from all regions and generate a global revenue report."""
    regions = ['US-East', 'US-West', 'EU', 'APAC']
    dfs = [pd.read_json(context['ti'].xcom_pull(task_ids=f'convert_{region}_data')) for region in regions]
    combined_df = pd.concat(dfs)
    combined_df['revenue'] = combined_df['amount'] * combined_df['quantity']
    total_revenue = combined_df['revenue'].sum()
    report = f"Total Global Revenue: {total_revenue} USD"
    with open('/path/to/global_revenue_report.csv', 'w') as f:
        f.write(report)
    return report

with DAG(
    dag_id='multi_region_ecommerce_pipeline',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    start_pipeline = EmptyOperator(task_id='start_pipeline')

    ingest_us_east_data = PythonOperator(
        task_id='ingest_us_east_data',
        python_callable=ingest_sales_data,
        op_kwargs={'region': 'US-East'},
    )

    ingest_us_west_data = PythonOperator(
        task_id='ingest_us_west_data',
        python_callable=ingest_sales_data,
        op_kwargs={'region': 'US-West'},
    )

    ingest_eu_data = PythonOperator(
        task_id='ingest_eu_data',
        python_callable=ingest_sales_data,
        op_kwargs={'region': 'EU'},
    )

    ingest_apac_data = PythonOperator(
        task_id='ingest_apac_data',
        python_callable=ingest_sales_data,
        op_kwargs={'region': 'APAC'},
    )

    convert_us_east_data = PythonOperator(
        task_id='convert_us_east_data',
        python_callable=convert_currency,
        op_kwargs={'region': 'US-East', 'from_currency': 'USD', 'to_currency': 'USD'},
    )

    convert_us_west_data = PythonOperator(
        task_id='convert_us_west_data',
        python_callable=convert_currency,
        op_kwargs={'region': 'US-West', 'from_currency': 'USD', 'to_currency': 'USD'},
    )

    convert_eu_data = PythonOperator(
        task_id='convert_eu_data',
        python_callable=convert_currency,
        op_kwargs={'region': 'EU', 'from_currency': 'EUR', 'to_currency': 'USD'},
    )

    convert_apac_data = PythonOperator(
        task_id='convert_apac_data',
        python_callable=convert_currency,
        op_kwargs={'region': 'APAC', 'from_currency': 'JPY', 'to_currency': 'USD'},
    )

    aggregate_data_task = PythonOperator(
        task_id='aggregate_data',
        python_callable=aggregate_data,
        provide_context=True,
    )

    end_pipeline = EmptyOperator(task_id='end_pipeline')

    start_pipeline >> [ingest_us_east_data, ingest_us_west_data, ingest_eu_data, ingest_apac_data]
    [ingest_us_east_data, ingest_us_west_data, ingest_eu_data, ingest_apac_data] >> [convert_us_east_data, convert_us_west_data, convert_eu_data, convert_apac_data]
    [convert_us_east_data, convert_us_west_data, convert_eu_data, convert_apac_data] >> aggregate_data_task
    aggregate_data_task >> end_pipeline