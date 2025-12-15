from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='ingest_csv_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

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

    # Define task dependencies
    ingest_csv >> quality_check
    quality_check >> production_load
    quality_check >> quarantine_and_alert
    quarantine_and_alert >> send_alert_email
    [production_load, send_alert_email] >> cleanup