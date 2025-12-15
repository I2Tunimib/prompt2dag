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
    dag_id='analyze_transactions_pipeline',
    default_args=default_args,
    description='No description provided.',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    analyze_transactions = DockerOperator(
        task_id='analyze_transactions',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    route_transaction = DockerOperator(
        task_id='route_transaction',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    route_to_auto_approve = DockerOperator(
        task_id='route_to_auto_approve',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    route_to_manual_review = DockerOperator(
        task_id='route_to_manual_review',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    send_notification = DockerOperator(
        task_id='send_notification',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    analyze_transactions >> route_transaction
    route_transaction >> route_to_manual_review
    route_transaction >> route_to_auto_approve
    route_to_manual_review >> send_notification
    route_to_auto_approve >> send_notification