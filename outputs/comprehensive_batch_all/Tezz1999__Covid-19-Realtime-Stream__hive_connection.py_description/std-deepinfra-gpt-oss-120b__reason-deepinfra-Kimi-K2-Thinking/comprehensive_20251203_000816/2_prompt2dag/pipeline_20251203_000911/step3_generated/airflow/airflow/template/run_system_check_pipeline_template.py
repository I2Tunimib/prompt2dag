# ==============================================================================
# Generated Airflow DAG
# Pipeline: run_system_check_pipeline
# Pattern: sequential
# Strategy: template
# Generated: 2025-12-03T00:11:23.448102
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
    dag_id='run_system_check_pipeline',
    default_args=default_args,
    description='This is a simple linear data pipeline that executes Hive database operations for COVID-19 realtime streaming data. The pipeline follows a sequential topology pattern with two tasks executing in strict order. Key infrastructure features include Hive database connectivity and scheduled daily execution at 1:00 AM.',
    schedule_interval='00 1 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['generated', 'template', 'sequential'],
) as dag:

    # ==========================================================================
    # Task Definitions
    # ==========================================================================

    # Task: run_system_check
    run_system_check = DockerOperator(
        task_id='run_system_check',
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

    # Task: execute_hive_script
    execute_hive_script = DockerOperator(
        task_id='execute_hive_script',
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
    run_system_check >> execute_hive_script
