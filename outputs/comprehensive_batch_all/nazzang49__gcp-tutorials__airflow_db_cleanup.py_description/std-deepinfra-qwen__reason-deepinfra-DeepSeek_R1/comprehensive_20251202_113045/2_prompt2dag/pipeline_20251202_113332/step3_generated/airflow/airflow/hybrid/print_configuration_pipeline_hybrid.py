from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='print_configuration_pipeline',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    # Task: print_configuration
    print_configuration = DockerOperator(
        task_id='print_configuration',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task: cleanup_airflow_metadb
    cleanup_airflow_metadb = DockerOperator(
        task_id='cleanup_airflow_metadb',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Set task dependencies
    print_configuration >> cleanup_airflow_metadb