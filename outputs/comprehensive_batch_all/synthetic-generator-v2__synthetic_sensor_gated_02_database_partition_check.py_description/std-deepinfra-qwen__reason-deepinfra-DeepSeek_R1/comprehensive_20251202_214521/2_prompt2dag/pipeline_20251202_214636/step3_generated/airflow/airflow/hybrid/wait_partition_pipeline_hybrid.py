from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

# Define the DAG
with DAG(
    dag_id='wait_partition_pipeline',
    description='Comprehensive Pipeline Description for Database Partition Check ETL',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    # Task definitions
    wait_partition = DockerOperator(
        task_id='wait_partition',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    extract_incremental = DockerOperator(
        task_id='extract_incremental',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    transform = DockerOperator(
        task_id='transform',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    load = DockerOperator(
        task_id='load',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task dependencies
    wait_partition >> extract_incremental >> transform >> load