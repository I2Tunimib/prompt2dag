# ==============================================================================
# Generated Airflow DAG
# Pipeline: AirVisual_Pipeline_Lat_Long_v1
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-02T05:35:12.576749
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
    dag_id='AirVisual_Pipeline_Lat_Long_v1',
    default_args=default_args,
    description='No description provided.',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['generated', 'template', 'sequential'],
) as dag:

    # ==========================================================================
    # Task Definitions
    # ==========================================================================

    # Task: get_airvisual_data_hourly
    get_airvisual_data_hourly = DockerOperator(
        task_id='get_airvisual_data_hourly',
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

    # Task: read_data_airvisual
    read_data_airvisual = DockerOperator(
        task_id='read_data_airvisual',
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

    # Task: load_data_airvisual_to_postgresql
    load_data_airvisual_to_postgresql = DockerOperator(
        task_id='load_data_airvisual_to_postgresql',
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
    get_airvisual_data_hourly >> read_data_airvisual
    read_data_airvisual >> load_data_airvisual_to_postgresql
