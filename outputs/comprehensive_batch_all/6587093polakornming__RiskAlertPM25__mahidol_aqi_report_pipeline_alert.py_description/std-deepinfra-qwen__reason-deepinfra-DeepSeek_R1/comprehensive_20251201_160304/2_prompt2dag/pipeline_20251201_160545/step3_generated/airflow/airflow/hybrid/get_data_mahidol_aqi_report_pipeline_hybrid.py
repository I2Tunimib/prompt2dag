from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='get_data_mahidol_aqi_report_pipeline',
    description='No description provided.',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
) as dag:

    # Task definitions
    get_data_mahidol_aqi_report = DockerOperator(
        task_id='get_data_mahidol_aqi_report',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    create_json_object = DockerOperator(
        task_id='create_json_object',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    load_mahidol_aqi_to_postgres = DockerOperator(
        task_id='load_mahidol_aqi_to_postgres',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    alert_email = DockerOperator(
        task_id='alert_email',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task dependencies
    get_data_mahidol_aqi_report >> create_json_object >> load_mahidol_aqi_to_postgres >> alert_email