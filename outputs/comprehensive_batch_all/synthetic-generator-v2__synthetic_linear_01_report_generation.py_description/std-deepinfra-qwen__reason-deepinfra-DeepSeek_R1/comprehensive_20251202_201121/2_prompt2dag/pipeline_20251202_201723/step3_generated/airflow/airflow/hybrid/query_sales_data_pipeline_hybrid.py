from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from datetime import datetime, timedelta

# Define the DAG
with DAG(
    dag_id='query_sales_data_pipeline',
    description='Comprehensive pipeline that generates daily sales reports by querying PostgreSQL sales data, transforming it into CSV format, creating a PDF chart visualization, and emailing the final report to management.',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    # Task: query_sales_data
    query_sales_data = DockerOperator(
        task_id='query_sales_data',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task: transform_to_csv
    transform_to_csv = DockerOperator(
        task_id='transform_to_csv',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task: generate_pdf_chart
    generate_pdf_chart = DockerOperator(
        task_id='generate_pdf_chart',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task: email_sales_report
    email_sales_report = DockerOperator(
        task_id='email_sales_report',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Set task dependencies
    query_sales_data >> transform_to_csv >> generate_pdf_chart >> email_sales_report