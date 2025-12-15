from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='wait_for_sales_aggregation_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
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