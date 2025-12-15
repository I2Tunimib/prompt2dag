from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="climate_data_fusion_pipeline",
    default_args=default_args,
    description="Implements a climate data fusion workflow that downloads weather station data from five meteorological agencies, normalizes each dataset, and merges them into a unified climate dataset.",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["climate", "data-fusion"],
) as dag:

    download_bom = DockerOperator(
        task_id='download_bom',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    download_ecmwf = DockerOperator(
        task_id='download_ecmwf',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    download_jma = DockerOperator(
        task_id='download_jma',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    download_metoffice = DockerOperator(
        task_id='download_metoffice',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    download_noaa = DockerOperator(
        task_id='download_noaa',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    normalize_bom = DockerOperator(
        task_id='normalize_bom',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    normalize_ecmwf = DockerOperator(
        task_id='normalize_ecmwf',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    normalize_jma = DockerOperator(
        task_id='normalize_jma',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    normalize_metoffice = DockerOperator(
        task_id='normalize_metoffice',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    normalize_noaa = DockerOperator(
        task_id='normalize_noaa',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    merge_climate_data = DockerOperator(
        task_id='merge_climate_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Define dependencies
    download_bom >> normalize_bom
    download_ecmwf >> normalize_ecmwf
    download_jma >> normalize_jma
    download_metoffice >> normalize_metoffice
    download_noaa >> normalize_noaa

    (
        normalize_bom,
        normalize_ecmwf,
        normalize_jma,
        normalize_metoffice,
        normalize_noaa,
    ) >> merge_climate_data