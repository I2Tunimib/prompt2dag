from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

# Define the DAG
with DAG(
    dag_id='read_csv_pipeline',
    description='No description provided.',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task definitions
    read_csv = DockerOperator(
        task_id='read_csv',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    account_check = DockerOperator(
        task_id='account_check',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    route_to_fatca = DockerOperator(
        task_id='route_to_fatca',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    route_to_irs = DockerOperator(
        task_id='route_to_irs',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    archive_reports = DockerOperator(
        task_id='archive_reports',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task dependencies
    read_csv >> account_check
    account_check >> route_to_fatca
    account_check >> route_to_irs
    [route_to_fatca, route_to_irs] >> archive_reports