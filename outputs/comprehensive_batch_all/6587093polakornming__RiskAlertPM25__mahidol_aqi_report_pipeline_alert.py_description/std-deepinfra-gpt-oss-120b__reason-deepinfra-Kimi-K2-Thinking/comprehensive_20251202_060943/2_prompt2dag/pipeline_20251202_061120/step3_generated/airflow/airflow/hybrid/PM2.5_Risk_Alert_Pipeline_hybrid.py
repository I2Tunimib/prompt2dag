from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 300,
}

with DAG(
    dag_id="PM2_5_Risk_Alert_Pipeline",
    description="Comprehensive Pipeline Description",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["pm25", "risk", "alert"],
) as dag:

    # Provided task definitions
    extract_mahidol_aqi_html = DockerOperator(
        task_id='extract_mahidol_aqi_html',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    transform_mahidol_aqi_json = DockerOperator(
        task_id='transform_mahidol_aqi_json',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    load_mahidol_aqi_postgres = DockerOperator(
        task_id='load_mahidol_aqi_postgres',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    notify_pm25_email_alert = DockerOperator(
        task_id='notify_pm25_email_alert',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Implicit branch placeholders (not provided in the original snippets)
    data_freshness_branch = DummyOperator(task_id='data_freshness_branch')
    aqi_threshold_branch = DummyOperator(task_id='aqi_threshold_branch')

    # Define dependencies (sequential pattern with branching)
    extract_mahidol_aqi_html >> transform_mahidol_aqi_json

    transform_mahidol_aqi_json >> data_freshness_branch
    transform_mahidol_aqi_json >> aqi_threshold_branch

    data_freshness_branch >> load_mahidol_aqi_postgres
    aqi_threshold_branch >> notify_pm25_email_alert