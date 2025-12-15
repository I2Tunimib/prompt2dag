from datetime import datetime

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 300,
}

with DAG(
    dag_id="extract_user_content_pipeline",
    description="Content Moderation Pipeline",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["fanout_fanin"],
) as dag:

    # Task definitions (provided exactly)

    extract_user_content = DockerOperator(
        task_id='extract_user_content',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    evaluate_toxicity = DockerOperator(
        task_id='evaluate_toxicity',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    publish_content = DockerOperator(
        task_id='publish_content',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    remove_and_flag_content = DockerOperator(
        task_id='remove_and_flag_content',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    audit_log = DockerOperator(
        task_id='audit_log',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Dependencies (fanout_fanin pattern)
    extract_user_content >> evaluate_toxicity
    evaluate_toxicity >> [remove_and_flag_content, publish_content]
    [remove_and_flag_content, publish_content] >> audit_log