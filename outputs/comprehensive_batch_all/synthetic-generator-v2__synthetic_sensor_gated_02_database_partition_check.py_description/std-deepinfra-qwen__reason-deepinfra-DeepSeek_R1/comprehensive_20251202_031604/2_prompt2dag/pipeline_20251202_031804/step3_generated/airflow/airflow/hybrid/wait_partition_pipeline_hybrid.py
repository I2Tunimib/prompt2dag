from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

# Define the DAG
with DAG(
    dag_id='wait_partition_pipeline',
    description='This is a sensor-gated daily ETL pipeline that waits for database partition availability before extracting, transforming, and loading incremental orders data.',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task: wait_partition
    wait_partition = DockerOperator(
        task_id='wait_partition',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task: extract_incremental
    extract_incremental = DockerOperator(
        task_id='extract_incremental',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task: transform
    transform = DockerOperator(
        task_id='transform',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task: load
    load = DockerOperator(
        task_id='load',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Set task dependencies
    wait_partition >> extract_incremental >> transform >> load