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
    dag_id='run_ctas_pipeline',
    default_args=default_args,
    description='No description provided.',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    run_ctas = DockerOperator(
        task_id='run_ctas',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Since the pattern is sequential and run_ctas has no dependencies, it is the entry point
    run_ctas