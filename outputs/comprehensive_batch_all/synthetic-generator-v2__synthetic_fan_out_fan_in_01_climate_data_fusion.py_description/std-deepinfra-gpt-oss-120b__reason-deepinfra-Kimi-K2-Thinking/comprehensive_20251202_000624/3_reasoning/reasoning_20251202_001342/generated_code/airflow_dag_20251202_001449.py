from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
from typing import List

default_args = {
    'owner': 'climate-team',
    'depends_on_past': False,
    'email': ['climate-alerts@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='climate_data_fusion_pipeline',
    default_args=default_args,
    description='Climate data fusion pipeline with fan-out/fan-in pattern',
    schedule_interval='0 2 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['climate', 'data-fusion', 'weather'],
) as dag:

    def download_noaa(**context):
        """Simulate downloading NOAA weather station data from FTP."""
        file_path = f"/tmp/noaa_data_{context['ds_nodash']}.csv"
        with open(file_path, 'w') as f:
            f.write("timestamp,temperature_f,elevation_ft\n2023-01-01 00:00:00,32.0,100.0\n")
        context['task_instance'].xcom_push(key='file_path', value=file_path)
        return file_path

    def download_ecmwf(**context):
        """Simulate downloading ECMWF weather station data from HTTPS endpoint."""
        file_path = f"/tmp/ecmwf_data_{context['ds_nodash']}.csv"
        with open(file_path, 'w') as f:
            f.write("time,temp_k,height_m\n2023-01-01T00:00:00Z,273.15,30.0\n")
        context['task_instance'].xcom_push(key='file_path', value=file_path)
        return file_path

    def download_jma(**context):
        """Simulate downloading JMA weather station data from HTTPS endpoint."""
        file_path = f"/tmp/jma_data_{context['ds_nodash']}.csv"
        with open(file_path, 'w') as f:
            f.write("datetime,temperature_c,elevation_m\n2023-01-01 00:00:00,0.0,50.0\n")
        context['task_instance'].xcom_push(key='file_path', value=file_path)
        return file_path

    def download_metoffice(**context):
        """Simulate downloading MetOffice weather station data from HTTPS endpoint."""
        file_path = f"/tmp/metoffice_data_{context['ds_nodash']}.csv"
        with open(file_path, 'w') as f:
            f.write("date,temp_c,altitude_m\n2023-01-01 00:00:00,5.0,75.0\n")
        context['task_instance'].xcom_push(key='file_path', value=file_path)
        return file_path

    def download_bom(**context):
        """Simulate downloading BOM weather station data from HTTPS endpoint."""
        file_path = f"/tmp/bom_data_{context['ds_nodash']}.csv"
        with open(file_path, 'w') as f:
            f.write("observation_time,temp_c,station_height_m\n2023-01-01 00:00:00,25.0,10.0\n")
        context['task_instance'].xcom_push(key='file_path', value=file_path)
        return file_path

    def normalize_noaa(**context):
        """Convert NOAA data to standard format (ISO timestamp, Celsius, meters)."""
        ti = context['task_instance']
        input_path = ti.xcom_pull(task_ids='download_noaa', key='file_path')
        df = pd.read_csv(input_path)
        df['temperature_c'] = (df['temperature_f'] - 32) * 5 / 9
        df['elevation_m'] = df['elevation_ft'] * 0.3048
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        output_path = f"/tmp/normalized_noaa_{context['ds_nodash']}.csv"
        df[['timestamp', 'temperature_c', 'elevation_m']].to_csv(output_path, index=False)
        ti.xcom_push(key='normalized_path', value=output_path)
        return output_path

    def normalize_ecmwf(**context):
        """Convert ECMWF data to standard format (ISO timestamp, Celsius, meters)."""
        ti = context['task_instance']
        input_path = ti.xcom_pull(task_ids='download_ecmwf', key='file_path')
        df = pd.read_csv(input_path)
        df['temperature_c'] = df['temp_k'] - 273.15
        df['elevation_m'] = df['height_m']
        df['timestamp'] = pd.to_datetime(df['time']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        output_path = f"/tmp/normalized_ecmwf_{context['ds_nodash']}.csv"
        df[['timestamp', 'temperature_c', 'elevation_m']].to_csv(output_path, index=False)
        ti.xcom_push(key='normalized_path', value=output_path)
        return output_path

    def normalize_jma(**context):
        """Convert JMA data to standard format (ISO timestamp, Celsius, meters)."""
        ti = context['task_instance']
        input_path = ti.xcom_pull(task_ids='download_jma', key='file_path')
        df = pd.read_csv(input_path)
        df['timestamp'] = pd.to_datetime(df['datetime']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        output_path = f"/tmp/normalized_jma_{context['ds_nodash']}.csv"
        df[['timestamp', 'temperature_c', 'elevation_m']].to_csv(output_path, index=False)
        ti.xcom_push(key='normalized_path', value=output_path)
        return output_path

    def normalize_metoffice(**context):
        """Convert MetOffice data to standard format (ISO timestamp, Celsius, meters)."""
        ti = context['task_instance']
        input_path = ti.xcom_pull(task_ids='download_metoffice', key='file_path')
        df = pd.read_csv(input_path)
        df['timestamp'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        output_path = f"/tmp/normalized_metoffice_{context['ds_nodash']}.csv"
        df[['timestamp', 'temperature_c', 'elevation_m']].to_csv(output_path, index=False)
        ti.xcom_push(key='normalized_path', value=output_path)
        return output_path

    def normalize_bom(**context):
        """Convert BOM data to standard format (ISO timestamp, Celsius, meters)."""
        ti = context['task_instance']
        input_path = ti.xcom_pull(task_ids='download_bom', key='file_path')
        df = pd.read_csv(input_path)
        df['timestamp'] = pd.to_datetime(df['observation_time']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        output_path = f"/tmp/normalized_bom_{context['ds_nodash']}.csv"
        df[['timestamp', 'temperature_c', 'elevation_m']].to_csv(output_path, index=False)
        ti.xcom_push(key='normalized_path', value=output_path)
        return output_path

    def merge_climate_data(**context):
        """Merge all normalized datasets into unified Parquet file."""
        ti = context['task_instance']
        agencies = ['noaa', 'ecmwf', 'jma', 'metoffice', 'bom']
        dataframes = []
        for agency in agencies:
            normalized_path = ti.xcom_pull(task_ids=f'normalize_{agency}', key='normalized_path')
            if normalized_path and os.path.exists(normalized_path):
                df = pd.read_csv(normalized_path)
                df['source_agency'] = agency.upper()
                dataframes.append(df)
        if not dataframes:
            raise ValueError("No normalized data found for merging")
        merged_df = pd.concat(dataframes, ignore_index=True)
        output_path = f"/tmp/unified_climate_data_{context['ds_nodash']}.parquet"
        merged_df.to_parquet(output_path, index=False)
        ti.xcom_push(key='merged_path', value=output_path)
        return output_path

    # Define download tasks
    download_noaa_task = PythonOperator(task_id='download_noaa', python_callable=download_noaa)
    download_ecmwf_task = PythonOperator(task_id='download_ecmwf', python_callable=download_ecmwf)
    download_jma_task = PythonOperator(task_id='download_jma', python_callable=download_jma)
    download_metoffice_task = PythonOperator(task_id='download_metoffice', python_callable=download_metoffice)
    download_bom_task = PythonOperator(task_id='download_bom', python_callable=download_bom)

    # Define normalization tasks
    normalize_noaa_task = PythonOperator(task_id='normalize_noaa', python_callable=normalize_noaa)
    normalize_ecmwf_task = PythonOperator(task_id='normalize_ecmwf', python_callable=normalize_ecmwf)
    normalize_jma_task = PythonOperator(task_id='normalize_jma', python_callable=normalize_jma)
    normalize_metoffice_task = PythonOperator(task_id='normalize_metoffice', python_callable=normalize_metoffice)
    normalize_bom_task = PythonOperator(task_id='normalize_bom', python_callable=normalize_bom)

    # Define merge task
    merge_climate_data_task = PythonOperator(task_id='merge_climate_data', python_callable=merge_climate_data)

    # Set dependencies: fan-out download -> normalize, then fan-in to merge
    download_noaa_task >> normalize_noaa_task
    download_ecmwf_task >> normalize_ecmwf_task
    download_jma_task >> normalize_jma_task
    download_metoffice_task >> normalize_metoffice_task
    download_bom_task >> normalize_bom_task

    [normalize_noaa_task, normalize_ecmwf_task, normalize_jma_task, normalize_metoffice_task, normalize_bom_task] >> merge_climate_data_task