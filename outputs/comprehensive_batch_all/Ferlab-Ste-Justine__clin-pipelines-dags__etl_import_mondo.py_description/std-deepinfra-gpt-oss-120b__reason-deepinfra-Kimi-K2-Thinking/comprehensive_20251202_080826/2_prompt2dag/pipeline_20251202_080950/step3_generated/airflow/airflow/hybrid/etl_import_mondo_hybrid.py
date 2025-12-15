from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="etl_import_mondo",
    default_args=default_args,
    description="Comprehensive Pipeline Description",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["etl"],
) as dag:

    validate_color_parameter = DockerOperator(
        task_id="validate_color_parameter",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    download_mondo_terms = DockerOperator(
        task_id="download_mondo_terms",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    normalize_mondo_terms = DockerOperator(
        task_id="normalize_mondo_terms",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    index_mondo_terms = DockerOperator(
        task_id="index_mondo_terms",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    publish_mondo_data = DockerOperator(
        task_id="publish_mondo_data",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    send_slack_notification = DockerOperator(
        task_id="send_slack_notification",
        image="python:3.9",
        environment={},
        network_mode="bridge",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
    )

    # Set sequential dependencies
    validate_color_parameter >> download_mondo_terms >> normalize_mondo_terms >> index_mondo_terms >> publish_mondo_data >> send_slack_notification