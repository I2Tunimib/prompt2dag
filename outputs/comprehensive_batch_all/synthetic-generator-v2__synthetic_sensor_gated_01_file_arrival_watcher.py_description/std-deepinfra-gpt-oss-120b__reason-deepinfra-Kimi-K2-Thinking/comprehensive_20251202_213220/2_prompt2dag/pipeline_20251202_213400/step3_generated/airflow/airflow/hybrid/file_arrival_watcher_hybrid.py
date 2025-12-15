from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='file_arrival_watcher',
    default_args=default_args,
    description='Monitors daily transaction file arrivals, validates schema, and loads data to PostgreSQL.',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['file', 'validation', 'postgres'],
) as dag:

    wait_for_file = DockerOperator(
        task_id='wait_for_file',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    validate_schema = DockerOperator(
        task_id='validate_schema',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    load_db = DockerOperator(
        task_id='load_db',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    wait_for_file >> validate_schema >> load_db