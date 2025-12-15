# ==============================================================================
# Generated Airflow DAG
# Pipeline: check_pcd_sftp_folder_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T00:28:47.098547
# ==============================================================================

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.helpers import chain
from docker.types import Mount

# --- Configuration ---
HOST_DATA_DIR = os.getenv('HOST_DATA_DIR', '/tmp/airflow/data')
CONTAINER_DATA_DIR = '/app/data'

# --- Default Arguments ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- DAG Definition ---
with DAG(
    dag_id='check_pcd_sftp_folder_pipeline',
    default_args=default_args,
    description='Comprehensive Pipeline Description',
    schedule_interval='{{var.value.pcd_etl_schedule}}',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['generated', 'template', 'sequential'],
) as dag:

    # ==========================================================================
    # Task Definitions
    # ==========================================================================

    # Task: check_pcd_sftp_folder
    check_pcd_sftp_folder = DockerOperator(
        task_id='check_pcd_sftp_folder',
        image=Undefined,
        environment={},
        network_mode=Undefined,
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=,
        docker_url=Undefined,
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: check_pcd_shared_folder
    check_pcd_shared_folder = DockerOperator(
        task_id='check_pcd_shared_folder',
        image=Undefined,
        environment={},
        network_mode=Undefined,
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=,
        docker_url=Undefined,
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: start_pcd_extract_1
    start_pcd_extract_1 = DockerOperator(
        task_id='start_pcd_extract_1',
        image=Undefined,
        environment={},
        network_mode=Undefined,
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=,
        docker_url=Undefined,
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: parallel_http_api_extraction
    parallel_http_api_extraction = DockerOperator(
        task_id='parallel_http_api_extraction',
        image=Undefined,
        environment={},
        network_mode=Undefined,
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=,
        docker_url=Undefined,
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: start_pcd_extract_2
    start_pcd_extract_2 = DockerOperator(
        task_id='start_pcd_extract_2',
        image=Undefined,
        environment={},
        network_mode=Undefined,
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=,
        docker_url=Undefined,
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: pcd_file_upload
    pcd_file_upload = DockerOperator(
        task_id='pcd_file_upload',
        image=Undefined,
        environment={},
        network_mode=Undefined,
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=,
        docker_url=Undefined,
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # Task: etl_notification
    etl_notification = DockerOperator(
        task_id='etl_notification',
        image=Undefined,
        environment={},
        network_mode=Undefined,
        mounts=[Mount(source=HOST_DATA_DIR, target=CONTAINER_DATA_DIR, type='bind')],
        auto_remove=,
        docker_url=Undefined,
        mount_tmp_dir=False,
        force_pull=False,
        tty=True,
    )

    # ==========================================================================
    # Task Dependencies
    # ==========================================================================
    check_pcd_sftp_folder >> check_pcd_shared_folder
    check_pcd_shared_folder >> start_pcd_extract_1
    start_pcd_extract_1 >> parallel_http_api_extraction
    parallel_http_api_extraction >> start_pcd_extract_2
    start_pcd_extract_2 >> pcd_file_upload
    pcd_file_upload >> etl_notification
