from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='fetch_user_data_pipeline',
    description='No description provided.',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,  # Schedule is disabled
) as dag:

    # Task definitions
    fetch_user_data = DockerOperator(
        task_id='fetch_user_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    process_user_data = DockerOperator(
        task_id='process_user_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    create_user_table = DockerOperator(
        task_id='create_user_table',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    insert_user_data = DockerOperator(
        task_id='insert_user_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task dependencies
    fetch_user_data >> process_user_data >> create_user_table >> insert_user_data