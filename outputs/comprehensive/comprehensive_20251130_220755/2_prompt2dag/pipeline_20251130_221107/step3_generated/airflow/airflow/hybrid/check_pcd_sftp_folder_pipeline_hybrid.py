from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.models import Variable

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='check_pcd_sftp_folder_pipeline',
    default_args=default_args,
    description='No description provided.',
    schedule_interval=Variable.get("pcd_etl_schedule"),
    start_date=days_ago(1),
    catchup=False,
    is_paused_upon_creation=False,
) as dag:

    # Task definitions
    check_pcd_sftp_folder = DockerOperator(
        task_id='check_pcd_sftp_folder',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    check_pcd_shared_folder = DockerOperator(
        task_id='check_pcd_shared_folder',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    start_pcd_extract_1 = DockerOperator(
        task_id='start_pcd_extract_1',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    parallel_http_api_extraction = DockerOperator(
        task_id='parallel_http_api_extraction',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    start_pcd_extract_2 = DockerOperator(
        task_id='start_pcd_extract_2',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    pcd_file_upload = DockerOperator(
        task_id='pcd_file_upload',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    etl_notification = DockerOperator(
        task_id='etl_notification',
        image='python:3.9',
        environment={},
        network_mode='bridge',
        auto_remove=True,
        docker_url='unix://var/run/docker.sock',
    )

    # Task dependencies
    check_pcd_sftp_folder >> check_pcd_shared_folder
    check_pcd_shared_folder >> start_pcd_extract_1
    start_pcd_extract_1 >> parallel_http_api_extraction
    parallel_http_api_extraction >> start_pcd_extract_2
    start_pcd_extract_2 >> pcd_file_upload
    pcd_file_upload >> etl_notification
    parallel_http_api_extraction >> etl_notification