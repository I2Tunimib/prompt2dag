from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="PM2.5_Risk_Alert_Pipeline",
    description="Sequential ETL pipeline that scrapes Mahidol University AQI data, transforms it to JSON, loads it into PostgreSQL, and sends email alerts when PM2.5 exceeds thresholds.",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    is_paused_upon_creation=True,
    tags=["etl", "aqi", "pm2.5"],
) as dag:

    # Entry task
    extract_mahidol_aqi_html = DockerOperator(
        task_id='extract_mahidol_aqi_html',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Transform task
    transform_html_to_json = DockerOperator(
        task_id='transform_html_to_json',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Branch placeholders (can be replaced with real branching logic later)
    duplicate_check_branch = DummyOperator(task_id='duplicate_check_branch')
    aqi_threshold_branch = DummyOperator(task_id='aqi_threshold_branch')

    # Load task
    load_mahidol_aqi_to_warehouse = DockerOperator(
        task_id='load_mahidol_aqi_to_warehouse',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Notification task
    notify_pm25_alert = DockerOperator(
        task_id='notify_pm25_alert',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Define sequential dependencies
    extract_mahidol_aqi_html >> transform_html_to_json

    # Branches after transformation
    transform_html_to_json >> duplicate_check_branch >> load_mahidol_aqi_to_warehouse
    transform_html_to_json >> aqi_threshold_branch >> notify_pm25_alert