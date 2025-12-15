from datetime import datetime, timedelta
import logging
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'climate-data-team',
    'depends_on_past': False,
    'email': ['climate-alerts@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}


def download_noaa(**context: Dict[str, Any]) -> str:
    """Fetches NOAA weather station CSV data from FTP server."""
    logger.info("Downloading NOAA weather station data")
    file_path = '/tmp/noaa_weather_data.csv'
    logger.info(f"NOAA data saved to {file_path}")
    return file_path


def download_ecmwf(**context: Dict[str, Any]) -> str:
    """Fetches ECMWF weather station CSV data from HTTPS endpoint."""
    logger.info("Downloading ECMWF weather station data")
    file_path = '/tmp/ecmwf_weather_data.csv'
    logger.info(f"ECMWF data saved to {file_path}")
    return file_path


def download_jma(**context: Dict[str, Any]) -> str:
    """Fetches JMA weather station CSV data from HTTPS endpoint."""
    logger.info("Downloading JMA weather station data")
    file_path = '/tmp/jma_weather_data.csv'
    logger.info(f"JMA data saved to {file_path}")
    return file_path


def download_metoffice(**context: Dict[str, Any]) -> str:
    """Fetches MetOffice weather station CSV data from HTTPS endpoint."""
    logger.info("Downloading MetOffice weather station data")
    file_path = '/tmp/metoffice_weather_data.csv'
    logger.info(f"MetOffice data saved to {file_path}")
    return file_path


def download_bom(**context: Dict[str, Any]) -> str:
    """Fetches BOM weather station CSV data from HTTPS endpoint."""
    logger.info("Downloading BOM weather station data")
    file_path = '/tmp/bom_weather_data.csv'
    logger.info(f"BOM data saved to {file_path}")
    return file_path


def normalize_noaa(**context: Dict[str, Any]) -> str:
    """Converts NOAA data to standard format (ISO timestamp, Celsius, meters)."""
    ti = context['task_instance']
    input_file = ti.xcom_pull(task_ids='download_noaa')
    logger.info(f"Normalizing NOAA data from {input_file}")
    output_file = '/tmp/noaa_normalized.csv'
    logger.info(f"NOAA normalized data saved to {output_file}")
    return output_file


def normalize_ecmwf(**context: Dict[str, Any]) -> str:
    """Converts ECMWF data to standard format."""
    ti = context['task_instance']
    input_file = ti.xcom_pull(task_ids='download_ecmwf')
    logger.info(f"Normalizing ECMWF data from {input_file}")
    output_file = '/tmp/ecmwf_normalized.csv'
    logger.info(f"ECMWF normalized data saved to {output_file}")
    return output_file


def normalize_jma(**context: Dict[str, Any]) -> str:
    """Converts JMA data to standard format."""
    ti = context['task_instance']
    input_file = ti.xcom_pull(task_ids='download_jma')
    logger.info(f"Normalizing JMA data from {input_file}")
    output_file = '/tmp/jma_normalized.csv'
    logger.info(f"JMA normalized data saved to {output_file}")
    return output_file


def normalize_metoffice(**context: Dict[str, Any]) -> str:
    """Converts MetOffice data to standard format."""
    ti = context['task_instance']
    input_file = ti.xcom_pull(task_ids='download_metoffice')
    logger.info(f"Normalizing MetOffice data from {input_file}")
    output_file = '/tmp/metoffice_normalized.csv'
    logger.info(f"MetOffice normalized data saved to {output_file}")
    return output_file


def normalize_bom(**context: Dict[str, Any]) -> str:
    """Converts BOM data to standard format."""
    ti = context['task_instance']
    input_file = ti.xcom_pull(task_ids='download_bom')
    logger.info(f"Normalizing BOM data from {input_file}")
    output_file = '/tmp/bom_normalized.csv'
    logger.info(f"BOM normalized data saved to {output_file}")
    return output_file


def merge_climate_data(**context: Dict[str, Any]) -> str:
    """Combines all normalized datasets into unified Parquet dataset."""
    ti = context['task_instance']
    logger.info("Starting merge of normalized climate datasets")
    
    normalized_files = {
        'noaa': ti.xcom_pull(task_ids='normalize_noaa'),
        'ecmwf': ti.xcom_pull(task_ids='normalize_ecmwf'),
        'jma': ti.xcom_pull(task_ids='normalize_jma'),
        'metoffice': ti.xcom_pull(task_ids='normalize_metoffice'),
        'bom': ti.xcom_pull(task_ids='normalize_bom'),
    }
    
    logger.info(f"Merging files: {normalized_files}")
    output_file = '/tmp/unified_climate_dataset.parquet'
    logger.info(f"Unified dataset saved to {output_file}")
    return output_file


with DAG(
    'climate_data_fusion_pipeline',
    default_args=default_args,
    description='Climate data fusion pipeline that downloads, normalizes, and merges weather station data from five meteorological agencies',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=5,
    tags=['climate', 'weather', 'data-fusion', 'etl'],
) as dag:
    
    # Download tasks (fan-out)
    download_noaa_task = PythonOperator(task_id='download_noaa', python_callable=download_noaa)
    download_ecmwf_task = PythonOperator(task_id='download_ecmwf', python_callable=download_ecmwf)
    download_jma_task = PythonOperator(task_id='download_jma', python_callable=download_jma)
    download_metoffice_task = PythonOperator(task_id='download_metoffice', python_callable=download_metoffice)
    download_bom_task = PythonOperator(task_id='download_bom', python_callable=download_bom)
    
    # Normalization tasks (fan-out)
    normalize_noaa_task = PythonOperator(task_id='normalize_noaa', python_callable=normalize_noaa)
    normalize_ecmwf_task = PythonOperator(task_id='normalize_ecmwf', python_callable=normalize_ecmwf)
    normalize_jma_task = PythonOperator(task_id='normalize_jma', python_callable=normalize_jma)
    normalize_metoffice_task = PythonOperator(task_id='normalize_metoffice', python_callable=normalize_metoffice)
    normalize_bom_task = PythonOperator(task_id='normalize_bom', python_callable=normalize_bom)
    
    # Merge task (fan-in)
    merge_climate_data_task = PythonOperator(task_id='merge_climate_data', python_callable=merge_climate_data)
    
    # Dependencies: download -> normalize -> merge
    download_noaa_task >> normalize_noaa_task
    download_ecmwf_task >> normalize_ecmwf_task
    download_jma_task >> normalize_jma_task
    download_metoffice_task >> normalize_metoffice_task
    download_bom_task >> normalize_bom_task
    
    [
        normalize_noaa_task,
        normalize_ecmwf_task,
        normalize_jma_task,
        normalize_metoffice_task,
        normalize_bom_task,
    ] >> merge_climate_data_task