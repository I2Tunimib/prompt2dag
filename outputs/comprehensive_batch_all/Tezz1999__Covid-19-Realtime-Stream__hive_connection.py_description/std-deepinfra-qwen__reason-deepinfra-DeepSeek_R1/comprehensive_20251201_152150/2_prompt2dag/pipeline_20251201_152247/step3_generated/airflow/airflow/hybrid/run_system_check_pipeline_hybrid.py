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
    dag_id='run_system_check_pipeline',
    default_args=default_args,
    description='No description provided.',
    schedule_interval='00 1 * * *',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    run_system_check = DockerOperator(
        task_id='run_system_check',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    execute_hive_script = DockerOperator(
        task_id='execute_hive_script',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    run_system_check >> execute_hive_script