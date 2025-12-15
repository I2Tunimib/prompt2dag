from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.email import EmailOperator
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='climate_data_fusion_pipeline',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
) as dag:

    def download_noaa():
        # Fetch NOAA weather station CSV data
        pass

    def download_ecmwf():
        # Fetch ECMWF weather station CSV data
        pass

    def download_jma():
        # Fetch JMA weather station CSV data
        pass

    def download_metoffice():
        # Fetch MetOffice weather station CSV data
        pass

    def download_bom():
        # Fetch BOM weather station CSV data
        pass

    def normalize_noaa():
        # Convert NOAA data to standard format
        pass

    def normalize_ecmwf():
        # Convert ECMWF data to standard format
        pass

    def normalize_jma():
        # Convert JMA data to standard format
        pass

    def normalize_metoffice():
        # Convert MetOffice data to standard format
        pass

    def normalize_bom():
        # Convert BOM data to standard format
        pass

    def merge_climate_data():
        # Combine all five normalized datasets into unified climate dataset
        pass

    # Download tasks
    download_noaa_task = PythonOperator(
        task_id='download_noaa',
        python_callable=download_noaa
    )

    download_ecmwf_task = PythonOperator(
        task_id='download_ecmwf',
        python_callable=download_ecmwf
    )

    download_jma_task = PythonOperator(
        task_id='download_jma',
        python_callable=download_jma
    )

    download_metoffice_task = PythonOperator(
        task_id='download_metoffice',
        python_callable=download_metoffice
    )

    download_bom_task = PythonOperator(
        task_id='download_bom',
        python_callable=download_bom
    )

    # Normalization tasks
    normalize_noaa_task = PythonOperator(
        task_id='normalize_noaa',
        python_callable=normalize_noaa
    )

    normalize_ecmwf_task = PythonOperator(
        task_id='normalize_ecmwf',
        python_callable=normalize_ecmwf
    )

    normalize_jma_task = PythonOperator(
        task_id='normalize_jma',
        python_callable=normalize_jma
    )

    normalize_metoffice_task = PythonOperator(
        task_id='normalize_metoffice',
        python_callable=normalize_metoffice
    )

    normalize_bom_task = PythonOperator(
        task_id='normalize_bom',
        python_callable=normalize_bom
    )

    # Merge task
    merge_climate_data_task = PythonOperator(
        task_id='merge_climate_data',
        python_callable=merge_climate_data
    )

    # Task dependencies
    download_noaa_task >> normalize_noaa_task
    download_ecmwf_task >> normalize_ecmwf_task
    download_jma_task >> normalize_jma_task
    download_metoffice_task >> normalize_metoffice_task
    download_bom_task >> normalize_bom_task

    [normalize_noaa_task, normalize_ecmwf_task, normalize_jma_task, normalize_metoffice_task, normalize_bom_task] >> merge_climate_data_task

    # Email notification on failure
    send_failure_email = EmailOperator(
        task_id='send_failure_email',
        to='admin@example.com',
        subject='Climate Data Fusion Pipeline Failed',
        html_content='The climate data fusion pipeline has failed. Please check the logs for more details.',
        trigger_rule='one_failed'
    )

    merge_climate_data_task >> send_failure_email