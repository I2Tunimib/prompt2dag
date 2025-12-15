from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='get_airvisual_data_hourly_pipeline',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
) as dag:

    # Task definitions
    get_airvisual_data_hourly = DockerOperator(
        task_id='get_airvisual_data_hourly',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    read_data_airvisual = DockerOperator(
        task_id='read_data_airvisual',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    load_data_airvisual_to_postgresql = DockerOperator(
        task_id='load_data_airvisual_to_postgresql',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task dependencies
    get_airvisual_data_hourly >> read_data_airvisual >> load_data_airvisual_to_postgresql