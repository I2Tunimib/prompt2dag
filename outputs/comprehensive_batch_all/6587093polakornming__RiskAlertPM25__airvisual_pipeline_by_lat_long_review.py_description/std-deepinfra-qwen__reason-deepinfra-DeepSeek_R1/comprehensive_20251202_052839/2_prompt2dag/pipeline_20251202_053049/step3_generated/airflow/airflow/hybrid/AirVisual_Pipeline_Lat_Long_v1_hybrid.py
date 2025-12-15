from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='AirVisual_Pipeline_Lat_Long_v1',
    description='No description provided.',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
) as dag:

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

    get_airvisual_data_hourly >> read_data_airvisual >> load_data_airvisual_to_postgresql