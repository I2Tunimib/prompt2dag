# ==============================================================================
# Generated Airflow DAG
# Pipeline: fetch_user_data_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-01T15:04:33.231988
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
    dag_id='fetch_user_data_pipeline',
    default_args=default_args,
    description='No description provided.',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=True,
    tags=['generated', 'template', 'sequential'],
) as dag:

    # ==========================================================================
    # Task Definitions
    # ==========================================================================

    # Task: fetch_user_data
    fetch_user_data = DockerOperator(
        task_id='fetch_user_data',
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

    # Task: process_user_data
    process_user_data = DockerOperator(
        task_id='process_user_data',
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

    # Task: create_user_table
    create_user_table = DockerOperator(
        task_id='create_user_table',
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

    # Task: insert_user_data
    insert_user_data = DockerOperator(
        task_id='insert_user_data',
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
    fetch_user_data >> process_user_data
    process_user_data >> create_user_table
    create_user_table >> insert_user_data
