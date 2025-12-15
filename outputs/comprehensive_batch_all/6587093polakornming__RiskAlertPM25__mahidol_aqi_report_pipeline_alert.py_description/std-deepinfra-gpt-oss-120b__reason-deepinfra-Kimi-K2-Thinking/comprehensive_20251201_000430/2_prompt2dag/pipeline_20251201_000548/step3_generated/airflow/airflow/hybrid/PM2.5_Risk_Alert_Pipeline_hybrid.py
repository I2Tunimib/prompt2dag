from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
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

    # ----------------------------------------------------------------------
    # Pre‑generated DockerOperator tasks (used exactly as provided)
    # ----------------------------------------------------------------------
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

    notify_pm25_email = DockerOperator(
        task_id='notify_pm25_email',
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

    # ----------------------------------------------------------------------
    # Additional placeholder tasks for branches referenced in dependencies
    # ----------------------------------------------------------------------
    duplicate_check_branch = DummyOperator(task_id="duplicate_check_branch")
    alert_decision_branch = DummyOperator(task_id="alert_decision_branch")

    # ----------------------------------------------------------------------
    # Define sequential workflow respecting the given dependencies
    # ----------------------------------------------------------------------
    extract_mahidol_aqi_html >> transform_mahidol_aqi_json

    transform_mahidol_aqi_json >> duplicate_check_branch >> load_mahidol_aqi_postgres
    transform_mahidol_aqi_json >> alert_decision_branch >> notify_pm25_email