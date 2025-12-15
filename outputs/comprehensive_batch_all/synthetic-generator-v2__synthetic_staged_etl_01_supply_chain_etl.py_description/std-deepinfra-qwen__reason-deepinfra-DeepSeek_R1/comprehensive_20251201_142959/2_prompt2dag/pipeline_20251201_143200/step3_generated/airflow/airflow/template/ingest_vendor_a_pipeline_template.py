# ==============================================================================
# Generated Airflow DAG - Fan-Out/Fan-In Pattern
# Pipeline: ingest_vendor_a_pipeline
# Pattern: fanin
# Strategy: template
# Generated: 2025-12-01T14:38:56.222055
# ==============================================================================

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
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
    dag_id='ingest_vendor_a_pipeline',
    default_args=default_args,
    description='No description provided.',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['generated', 'template', 'fanin'],
) as dag:

    # ==========================================================================
    # Task Definitions
    # ==========================================================================

    # Identify fan-out and fan-in points

    # Task: ingest_vendor_a
    ingest_vendor_a = DockerOperator(
        task_id='ingest_vendor_a',
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

    # Task: ingest_vendor_b
    ingest_vendor_b = DockerOperator(
        task_id='ingest_vendor_b',
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

    # Task: ingest_vendor_c
    ingest_vendor_c = DockerOperator(
        task_id='ingest_vendor_c',
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

    # Task: cleanse_data
    # 🔀 FAN-IN POINT: Multiple upstream tasks (trigger_rule=all_success)
    cleanse_data = DockerOperator(
        task_id='cleanse_data',
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

    # Task: load_to_db
    load_to_db = DockerOperator(
        task_id='load_to_db',
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

    # Task: send_summary_email
    send_summary_email = DockerOperator(
        task_id='send_summary_email',
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
    # Task Dependencies - Fan-Out/Fan-In Pattern
    # ==========================================================================
    # Fan-out points: None detected
    # Fan-in points: ['cleanse_data']

    ingest_vendor_a >> cleanse_data
    ingest_vendor_b >> cleanse_data
    ingest_vendor_c >> cleanse_data
    cleanse_data >> load_to_db
    load_to_db >> send_summary_email
