from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'climate-team',
    'depends_on_past': False,
    'email': ['climate-team@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='climate_data_fusion_pipeline',
    default_args=default_args,
    description='Climate data fusion pipeline that downloads, normalizes, and merges weather station data from five agencies',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['climate', 'data-fusion', 'weather'],
) as dag:

    def download_noaa(**context):
        """Fetches NOAA weather station CSV data from FTP server."""
        logger.info("Downloading NOAA weather station data")
        file_path = "/tmp/noaa_weather_data.csv"
        return file_path

    def download_ecmwf(**context):
        """Fetches ECMWF weather station CSV data from HTTPS endpoint."""
        logger.info("Downloading ECMWF weather station data")
        file_path = "/tmp/ecmwf_weather_data.csv"
        return file_path

    def download_jma(**context):
        """Fetches JMA weather station CSV data from HTTPS endpoint."""
        logger.info("Downloading JMA weather station data")
        file_path = "/tmp/jma_weather_data.csv"
        return file_path

    def download_metoffice(**context):
        """Fetches MetOffice weather station CSV data from HTTPS endpoint."""
        logger.info("Downloading MetOffice weather station data")
        file_path = "/tmp/metoffice_weather_data.csv"
        return file_path

    def download_bom(**context):
        """Fetches BOM weather station CSV data from HTTPS endpoint."""
        logger.info("Downloading BOM weather station data")
        file_path = "/tmp/bom_weather_data.csv"
        return file_path

    def normalize_noaa(**context):
        """Converts NOAA data to standard format (ISO timestamp, Celsius, meters)."""
        ti = context['task_instance']
        input_file = ti.xcom_pull(task_ids='download_noaa')
        logger.info(f"Normalizing NOAA data from {input_file}")
        output_file = "/tmp/noaa_normalized.parquet"
        return output_file

    def normalize_ecmwf(**context):
        """Converts ECMWF data to standard format (ISO timestamp, Celsius, meters)."""
        ti = context['task_instance']
        input_file = ti.xcom_pull(task_ids='download_ecmwf')
        logger.info(f"Normalizing ECMWF data from {input_file}")
        output_file = "/tmp/ecmwf_normalized.parquet"
        return output_file

    def normalize_jma(**context):
        """Converts JMA data to standard format (ISO timestamp, Celsius, meters)."""
        ti = context['task_instance']
        input_file = ti.xcom_pull(task_ids='download_jma')
        logger.info(f"Normalizing JMA data from {input_file}")
        output_file = "/tmp/jma_normalized.parquet"
        return output_file

    def normalize_metoffice(**context):
        """Converts MetOffice data to standard format (ISO timestamp, Celsius, meters)."""
        ti = context['task_instance']
        input_file = ti.xcom_pull(task_ids='download_metoffice')
        logger.info(f"Normalizing MetOffice data from {input_file}")
        output_file = "/tmp/metoffice_normalized.parquet"
        return output_file

    def normalize_bom(**context):
        """Converts BOM data to standard format (ISO timestamp, Celsius, meters)."""
        ti = context['task_instance']
        input_file = ti.xcom_pull(task_ids='download_bom')
        logger.info(f"Normalizing BOM data from {input_file}")
        output_file = "/tmp/bom_normalized.parquet"
        return output_file

    def merge_climate_data(**context):
        """Combines all five normalized datasets into unified climate dataset."""
        ti = context['task_instance']
        normalized_files = ti.xcom_pull(task_ids=[
            'normalize_noaa',
            'normalize_ecmwf',
            'normalize_jma',
            'normalize_metoffice',
            'normalize_bom'
        ])
        normalized_files = [f for f in normalized_files if f]
        logger.info(f"Merging climate data from files: {normalized_files}")
        output_file = "/tmp/unified_climate_dataset.parquet"
        return output_file

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
    )

    normalize_ecmwf_task = PythonOperator(
        task_id='normalize_ecmwf',
        python_callable=normalize_ecmwf,
    )

    normalize_jma_task = PythonOperator(
        task_id='normalize_jma',
        python_callable=normalize_jma,
    )

    normalize_metoffice_task = PythonOperator(
        task_id='normalize_metoffice',
        python_callable=normalize_metoffice,
    )

    normalize_bom_task = PythonOperator(
        task_id='normalize_bom',
        python_callable=normalize_bom,
    )

    merge_climate_data_task = PythonOperator(
        task_id='merge_climate_data',
        python_callable=merge_climate_data,
    )

    download_noaa_task >> normalize_noaa_task
    download_ecmwf_task >> normalize_ecmwf_task
    download_jma_task >> normalize_jma_task
    download_metoffice_task >> normalize_metoffice_task
    download_bom_task >> normalize_bom_task

    [normalize_noaa_task, normalize_ecmwf_task, normalize_jma_task, normalize_metoffice_task, normalize_bom_task] >> merge_climate_data_task