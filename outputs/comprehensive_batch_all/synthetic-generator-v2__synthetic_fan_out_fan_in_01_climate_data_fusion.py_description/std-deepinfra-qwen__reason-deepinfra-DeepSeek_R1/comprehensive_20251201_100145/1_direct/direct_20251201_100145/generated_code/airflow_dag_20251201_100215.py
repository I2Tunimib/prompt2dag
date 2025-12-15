from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def download_noaa():
    # Fetch NOAA weather station CSV data
    logging.info("Downloading NOAA data")
    # Placeholder for actual download logic
    return "path/to/noaa_data.csv"

def download_ecmwf():
    # Fetch ECMWF weather station CSV data
    logging.info("Downloading ECMWF data")
    # Placeholder for actual download logic
    return "path/to/ecmwf_data.csv"

def download_jma():
    # Fetch JMA weather station CSV data
    logging.info("Downloading JMA data")
    # Placeholder for actual download logic
    return "path/to/jma_data.csv"

def download_metoffice():
    # Fetch MetOffice weather station CSV data
    logging.info("Downloading MetOffice data")
    # Placeholder for actual download logic
    return "path/to/metoffice_data.csv"

def download_bom():
    # Fetch BOM weather station CSV data
    logging.info("Downloading BOM data")
    # Placeholder for actual download logic
    return "path/to/bom_data.csv"

def normalize_noaa(**context):
    # Convert NOAA data to standard format
    logging.info("Normalizing NOAA data")
    # Placeholder for actual normalization logic
    return "path/to/normalized_noaa_data.csv"

def normalize_ecmwf(**context):
    # Convert ECMWF data to standard format
    logging.info("Normalizing ECMWF data")
    # Placeholder for actual normalization logic
    return "path/to/normalized_ecmwf_data.csv"

def normalize_jma(**context):
    # Convert JMA data to standard format
    logging.info("Normalizing JMA data")
    # Placeholder for actual normalization logic
    return "path/to/normalized_jma_data.csv"

def normalize_metoffice(**context):
    # Convert MetOffice data to standard format
    logging.info("Normalizing MetOffice data")
    # Placeholder for actual normalization logic
    return "path/to/normalized_metoffice_data.csv"

def normalize_bom(**context):
    # Convert BOM data to standard format
    logging.info("Normalizing BOM data")
    # Placeholder for actual normalization logic
    return "path/to/normalized_bom_data.csv"

def merge_climate_data(**context):
    # Combine all five normalized datasets into unified climate dataset
    logging.info("Merging climate data")
    # Placeholder for actual merge logic
    return "path/to/unified_climate_data.parquet"

with DAG(
    'climate_data_fusion_pipeline',
    default_args=default_args,
    description='Climate data fusion pipeline',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
) as dag:

    download_noaa_task = PythonOperator(
        task_id='download_noaa',
        python_callable=download_noaa,
    )

    download_ecmwf_task = PythonOperator(
        task_id='download_ecmwf',
        python_callable=download_ecmwf,
    )

    download_jma_task = PythonOperator(
        task_id='download_jma',
        python_callable=download_jma,
    )

    download_metoffice_task = PythonOperator(
        task_id='download_metoffice',
        python_callable=download_metoffice,
    )

    download_bom_task = PythonOperator(
        task_id='download_bom',
        python_callable=download_bom,
    )

    normalize_noaa_task = PythonOperator(
        task_id='normalize_noaa',
        python_callable=normalize_noaa,
        provide_context=True,
    )

    normalize_ecmwf_task = PythonOperator(
        task_id='normalize_ecmwf',
        python_callable=normalize_ecmwf,
        provide_context=True,
    )

    normalize_jma_task = PythonOperator(
        task_id='normalize_jma',
        python_callable=normalize_jma,
        provide_context=True,
    )

    normalize_metoffice_task = PythonOperator(
        task_id='normalize_metoffice',
        python_callable=normalize_metoffice,
        provide_context=True,
    )

    normalize_bom_task = PythonOperator(
        task_id='normalize_bom',
        python_callable=normalize_bom,
        provide_context=True,
    )

    merge_climate_data_task = PythonOperator(
        task_id='merge_climate_data',
        python_callable=merge_climate_data,
        provide_context=True,
    )

    download_noaa_task >> normalize_noaa_task
    download_ecmwf_task >> normalize_ecmwf_task
    download_jma_task >> normalize_jma_task
    download_metoffice_task >> normalize_metoffice_task
    download_bom_task >> normalize_bom_task

    [normalize_noaa_task, normalize_ecmwf_task, normalize_jma_task, normalize_metoffice_task, normalize_bom_task] >> merge_climate_data_task