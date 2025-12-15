from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='create_customer_pipeline',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
) as dag:

    # Task: create_customer
    create_customer = DockerOperator(
        task_id='create_customer',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task: generate_customer_token
    generate_customer_token = DockerOperator(
        task_id='generate_customer_token',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task: get_customer_info
    get_customer_info = DockerOperator(
        task_id='get_customer_info',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Set task dependencies
    create_customer >> generate_customer_token >> get_customer_info