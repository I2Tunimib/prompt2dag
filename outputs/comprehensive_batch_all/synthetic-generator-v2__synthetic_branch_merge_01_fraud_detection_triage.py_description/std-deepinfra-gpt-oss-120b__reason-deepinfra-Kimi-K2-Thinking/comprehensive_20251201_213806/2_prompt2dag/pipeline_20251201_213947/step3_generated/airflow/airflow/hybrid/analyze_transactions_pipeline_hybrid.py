from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='analyze_transactions_pipeline',
    description='Comprehensive Pipeline Description',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=['fanout_fanin'],
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

    # Define dependencies (fanout_fanin pattern)
    analyze_transactions >> route_transaction
    route_transaction >> [route_to_manual_review, route_to_auto_approve]
    [route_to_manual_review, route_to_auto_approve] >> send_notification