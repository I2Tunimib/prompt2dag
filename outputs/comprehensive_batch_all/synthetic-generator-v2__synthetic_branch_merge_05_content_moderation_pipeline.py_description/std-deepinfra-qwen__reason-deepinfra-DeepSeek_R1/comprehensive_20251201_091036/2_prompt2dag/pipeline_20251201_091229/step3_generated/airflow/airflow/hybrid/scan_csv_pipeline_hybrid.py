from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    dag_id='scan_csv_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    scan_csv = DockerOperator(
        task_id='scan_csv',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    toxicity_check = DockerOperator(
        task_id='toxicity_check',
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

    scan_csv >> toxicity_check
    toxicity_check >> remove_and_flag_content
    toxicity_check >> publish_content
    [remove_and_flag_content, publish_content] >> audit_log