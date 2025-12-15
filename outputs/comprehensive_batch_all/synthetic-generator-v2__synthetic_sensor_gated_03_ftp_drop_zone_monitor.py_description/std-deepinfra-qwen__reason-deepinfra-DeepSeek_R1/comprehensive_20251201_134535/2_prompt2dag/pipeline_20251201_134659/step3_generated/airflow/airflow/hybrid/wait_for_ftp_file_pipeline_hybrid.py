from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

# Define the DAG
with DAG(
    dag_id='wait_for_ftp_file_pipeline',
    description='No description provided.',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task definitions
    wait_for_ftp_file = DockerOperator(
        task_id='wait_for_ftp_file',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    download_vendor_file = DockerOperator(
        task_id='download_vendor_file',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    cleanse_vendor_data = DockerOperator(
        task_id='cleanse_vendor_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    merge_with_internal_inventory = DockerOperator(
        task_id='merge_with_internal_inventory',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task dependencies
    wait_for_ftp_file >> download_vendor_file >> cleanse_vendor_data >> merge_with_internal_inventory