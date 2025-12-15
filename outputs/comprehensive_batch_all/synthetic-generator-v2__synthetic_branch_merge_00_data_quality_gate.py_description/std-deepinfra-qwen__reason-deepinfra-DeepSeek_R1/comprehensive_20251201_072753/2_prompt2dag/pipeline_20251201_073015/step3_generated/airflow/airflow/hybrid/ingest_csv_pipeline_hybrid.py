from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from datetime import datetime, timedelta

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ingest_csv_pipeline',
    default_args=default_args,
    description='Comprehensive Pipeline Description: This DAG implements a data quality gate for customer CSV data that ingests raw data, performs quality assessment, and conditionally routes to production or quarantine based on quality scores.',
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task definitions
    ingest_csv = DockerOperator(
        task_id='ingest_csv',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    quality_check = DockerOperator(
        task_id='quality_check',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    production_load = DockerOperator(
        task_id='production_load',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    quarantine_and_alert = DockerOperator(
        task_id='quarantine_and_alert',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    send_alert_email = DockerOperator(
        task_id='send_alert_email',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    cleanup = DockerOperator(
        task_id='cleanup',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task dependencies
    ingest_csv >> quality_check
    quality_check >> production_load
    quality_check >> quarantine_and_alert
    quarantine_and_alert >> send_alert_email
    [production_load, send_alert_email] >> cleanup