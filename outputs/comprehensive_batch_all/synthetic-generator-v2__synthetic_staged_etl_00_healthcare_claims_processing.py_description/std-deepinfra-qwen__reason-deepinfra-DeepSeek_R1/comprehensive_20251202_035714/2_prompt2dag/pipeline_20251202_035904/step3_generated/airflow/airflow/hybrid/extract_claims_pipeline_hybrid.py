from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

# Define the DAG
with DAG(
    dag_id='extract_claims_pipeline',
    description='No description provided.',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task definitions
    extract_claims = DockerOperator(
        task_id='extract_claims',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    extract_providers = DockerOperator(
        task_id='extract_providers',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    transform_join = DockerOperator(
        task_id='transform_join',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    load_warehouse = DockerOperator(
        task_id='load_warehouse',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    refresh_bi = DockerOperator(
        task_id='refresh_bi',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task dependencies
    extract_claims >> transform_join
    extract_providers >> transform_join
    transform_join >> load_warehouse
    transform_join >> refresh_bi