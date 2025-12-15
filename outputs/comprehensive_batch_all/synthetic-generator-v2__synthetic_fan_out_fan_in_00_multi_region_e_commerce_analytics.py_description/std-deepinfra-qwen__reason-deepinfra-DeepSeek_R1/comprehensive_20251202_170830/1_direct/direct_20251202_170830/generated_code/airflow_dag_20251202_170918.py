from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.email import EmailOperator
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['admin@example.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def ingest_sales_data(region):
    """Simulate ingesting sales data from a CSV file for a given region."""
    print(f"Ingesting sales data for {region} region.")
    return f"{region}_data"

def convert_currency(region, from_currency, to_currency):
    """Simulate converting currency for a given region."""
    if from_currency == to_currency:
        print(f"No conversion needed for {region} region.")
        return f"{region}_data"
    else:
        print(f"Converting {from_currency} to {to_currency} for {region} region.")
        return f"{region}_converted_data"

def aggregate_data(regions_data):
    """Simulate aggregating data from all regions and generating a global revenue report."""
    print("Aggregating data from all regions.")
    total_revenue = sum([float(data.split('_')[0]) for data in regions_data])
    print(f"Total global revenue: {total_revenue} USD")
    return f"global_revenue_report.csv"

with DAG(
    'multi_region_ecommerce_pipeline',
    default_args=default_args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    start_pipeline = EmptyOperator(task_id='start_pipeline')

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

    convert_us_east = PythonOperator(
        task_id='convert_us_east',
        python_callable=convert_currency,
        op_kwargs={'region': 'US-East', 'from_currency': 'USD', 'to_currency': 'USD'}
    )

    convert_us_west = PythonOperator(
        task_id='convert_us_west',
        python_callable=convert_currency,
        op_kwargs={'region': 'US-West', 'from_currency': 'USD', 'to_currency': 'USD'}
    )

    convert_eu = PythonOperator(
        task_id='convert_eu',
        python_callable=convert_currency,
        op_kwargs={'region': 'EU', 'from_currency': 'EUR', 'to_currency': 'USD'}
    )

    convert_apac = PythonOperator(
        task_id='convert_apac',
        python_callable=convert_currency,
        op_kwargs={'region': 'APAC', 'from_currency': 'JPY', 'to_currency': 'USD'}
    )

    aggregate_data_task = PythonOperator(
        task_id='aggregate_data',
        python_callable=aggregate_data,
        op_kwargs={
            'regions_data': [
                "{{ ti.xcom_pull(task_ids='convert_us_east') }}",
                "{{ ti.xcom_pull(task_ids='convert_us_west') }}",
                "{{ ti.xcom_pull(task_ids='convert_eu') }}",
                "{{ ti.xcom_pull(task_ids='convert_apac') }}"
            ]
        }
    )

    end_pipeline = EmptyOperator(task_id='end_pipeline')

    start_pipeline >> [ingest_us_east, ingest_us_west, ingest_eu, ingest_apac]
    [ingest_us_east, ingest_us_west, ingest_eu, ingest_apac] >> [convert_us_east, convert_us_west, convert_eu, convert_apac]
    [convert_us_east, convert_us_west, convert_eu, convert_apac] >> aggregate_data_task
    aggregate_data_task >> end_pipeline