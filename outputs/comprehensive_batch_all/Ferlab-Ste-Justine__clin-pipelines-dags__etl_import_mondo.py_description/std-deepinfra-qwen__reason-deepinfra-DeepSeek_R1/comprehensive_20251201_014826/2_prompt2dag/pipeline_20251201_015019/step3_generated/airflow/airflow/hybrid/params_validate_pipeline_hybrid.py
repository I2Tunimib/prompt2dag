from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

# Define the DAG
with DAG(
    dag_id='params_validate_pipeline',
    description='No description provided.',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Task definitions
    params_validate = DockerOperator(
        task_id='params_validate',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    download_mondo_terms = DockerOperator(
        task_id='download_mondo_terms',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    normalized_mondo_terms = DockerOperator(
        task_id='normalized_mondo_terms',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    index_mondo_terms = DockerOperator(
        task_id='index_mondo_terms',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    publish_mondo = DockerOperator(
        task_id='publish_mondo',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    slack = DockerOperator(
        task_id='slack',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task dependencies
    params_validate >> download_mondo_terms
    download_mondo_terms >> normalized_mondo_terms
    normalized_mondo_terms >> index_mondo_terms
    index_mondo_terms >> publish_mondo
    publish_mondo >> slack