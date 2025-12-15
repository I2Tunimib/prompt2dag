from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

# Define the DAG
with DAG(
    dag_id='run_after_loop_pipeline',
    description='No description provided.',
    schedule_interval='00 1 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,  # Schedule is disabled
) as dag:

    # Task definitions
    run_after_loop = DockerOperator(
        task_id='run_after_loop',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    hive_script_task = DockerOperator(
        task_id='hive_script_task',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Set task dependencies
    run_after_loop >> hive_script_task