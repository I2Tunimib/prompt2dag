from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='check_and_download_ensembl_files_pipeline',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
) as dag:

    # Task: check_and_download_ensembl_files
    check_and_download_ensembl_files = DockerOperator(
        task_id='check_and_download_ensembl_files',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task: process_ensembl_files_with_spark
    process_ensembl_files_with_spark = DockerOperator(
        task_id='process_ensembl_files_with_spark',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Set task dependencies
    check_and_download_ensembl_files >> process_ensembl_files_with_spark