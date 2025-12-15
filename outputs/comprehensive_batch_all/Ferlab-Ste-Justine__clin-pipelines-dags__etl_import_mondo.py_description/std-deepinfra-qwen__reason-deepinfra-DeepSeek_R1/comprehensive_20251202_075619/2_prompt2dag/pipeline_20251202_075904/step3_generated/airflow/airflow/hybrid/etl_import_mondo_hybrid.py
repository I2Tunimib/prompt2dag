from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
with DAG(
    dag_id='etl_import_mondo',
    default_args=default_args,
    schedule_interval=None,  # Schedule is disabled
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

    # Set task dependencies
    params_validate >> download_mondo_terms
    download_mondo_terms >> normalized_mondo_terms
    normalized_mondo_terms >> index_mondo_terms
    index_mondo_terms >> publish_mondo
    publish_mondo >> slack