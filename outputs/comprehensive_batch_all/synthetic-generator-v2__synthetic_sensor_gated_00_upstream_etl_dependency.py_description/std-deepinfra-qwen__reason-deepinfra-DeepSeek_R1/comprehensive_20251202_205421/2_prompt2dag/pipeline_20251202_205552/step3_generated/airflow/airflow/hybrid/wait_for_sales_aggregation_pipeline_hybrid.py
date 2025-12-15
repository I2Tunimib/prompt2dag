from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

with DAG(
    dag_id='wait_for_sales_aggregation_pipeline',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    wait_for_sales_aggregation = DockerOperator(
        task_id='wait_for_sales_aggregation',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    load_sales_csv = DockerOperator(
        task_id='load_sales_csv',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    generate_dashboard = DockerOperator(
        task_id='generate_dashboard',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    wait_for_sales_aggregation >> load_sales_csv >> generate_dashboard